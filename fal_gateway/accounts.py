"""Organization-scoped channel accounts; credentials never enter public projections."""
from __future__ import annotations

import hashlib
import hmac
import json
import uuid
from .proxy import proxy_reference
from decimal import Decimal, InvalidOperation
from datetime import datetime

from .store import Conflict, now


class Accounts:
    def __init__(self, store):
        self.store = store
        with store.connect() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS channel_accounts (
                  id TEXT PRIMARY KEY, organization_id TEXT NOT NULL,
                  name TEXT NOT NULL, api_url TEXT NOT NULL, proxy_json TEXT NOT NULL,
                  status TEXT NOT NULL, is_default INTEGER NOT NULL DEFAULT 0,
                  quota_subject_id TEXT NOT NULL, registration_json TEXT NOT NULL,
                  revision INTEGER NOT NULL DEFAULT 1, credential_version INTEGER NOT NULL,
                  authorization_state TEXT NOT NULL DEFAULT 'pending_verification',
                  created_at TEXT NOT NULL, updated_at TEXT NOT NULL
                );
                CREATE UNIQUE INDEX IF NOT EXISTS channel_account_default
                  ON channel_accounts(organization_id) WHERE is_default=1;
                CREATE TABLE IF NOT EXISTS channel_account_credentials (
                  account_id TEXT NOT NULL, version INTEGER NOT NULL,
                  ciphertext BLOB NOT NULL, created_at TEXT NOT NULL,
                  PRIMARY KEY(account_id, version)
                );
                CREATE TABLE IF NOT EXISTS channel_account_operations (
                  organization_id TEXT NOT NULL, operation_id TEXT NOT NULL,
                  digest TEXT NOT NULL, account_id TEXT NOT NULL,
                  PRIMARY KEY(organization_id, operation_id)
                );
                CREATE TABLE IF NOT EXISTS channel_account_audit (
                  id INTEGER PRIMARY KEY, organization_id TEXT NOT NULL,
                  account_id TEXT NOT NULL, actor TEXT NOT NULL,
                  action TEXT NOT NULL, revision INTEGER NOT NULL, created_at TEXT NOT NULL
                );
            """)

    @staticmethod
    def registration(value):
        if not isinstance(value, dict) or set(value) - {'balance', 'budget_limit', 'note', 'observed_at', 'daily_limit'}:
            raise ValueError('invalid_registration')
        result = dict(value)
        daily_limit = value.get('daily_limit')
        if daily_limit is not None and (type(daily_limit) is not int or not 0 <= daily_limit <= 1000):
            raise ValueError('invalid_daily_limit')
        for field in ('balance', 'budget_limit'):
            number = value.get(field)
            if number is None:
                continue
            if not isinstance(number, str) or len(number) > 32:
                raise ValueError('invalid_amount')
            try:
                decimal = Decimal(number)
                if (not decimal.is_finite() or decimal < 0 or decimal > Decimal('1e15')
                        or decimal.as_tuple().exponent < -8):
                    raise ValueError('invalid_amount')
            except InvalidOperation:
                raise ValueError('invalid_amount') from None
            result[field] = '0' if decimal == 0 and decimal.as_tuple().exponent > 0 else format(decimal, 'f')
        for field in ('note', 'observed_at'):
            if field in result and (not isinstance(result[field], str) or len(result[field]) > 2000):
                raise ValueError('invalid_registration')
        if result.get('observed_at'):
            try:
                timestamp = datetime.fromisoformat(result['observed_at'])
                if timestamp.tzinfo is None:
                    raise ValueError('observation_timezone_required')
            except ValueError:
                raise ValueError('invalid_observation_time') from None
        if result.get('balance') is not None and not result.get('observed_at'):
            result['observed_at'] = now()
        return result

    @staticmethod
    def project(row):
        return {'schema_version': 1, 'account_id': row['id'], 'name': row['name'],
                'api_url': row['api_url'], 'proxy': json.loads(row['proxy_json']),
                'status': row['status'], 'default': bool(row['is_default']),
                'quota_subject_id': row['quota_subject_id'],
                'registration': json.loads(row['registration_json']),
                'revision': row['revision'], 'authorization_state': row['authorization_state'],
                'created_at': row['created_at'], 'updated_at': row['updated_at']}

    @staticmethod
    def row(db, organization, account):
        row = db.execute('SELECT * FROM channel_accounts WHERE organization_id=? AND id=?',
                         (organization, account)).fetchone()
        if row is None:
            raise KeyError('account_not_found')
        return row

    def get(self, organization, account):
        with self.store.connect() as db:
            return self.project(self.row(db, organization, account))

    def credential(self, organization, account):
        revision, key, _ = self.connection(organization, account)
        return revision, key

    def connection(self, organization, account):
        with self.store.connect() as db:
            row = self.row(db, organization, account)
            if row['status'] == 'archived':
                raise Conflict('account_archived')
            credential = db.execute('SELECT ciphertext FROM channel_account_credentials '
                'WHERE account_id=? AND version=?', (account, row['credential_version'])).fetchone()
            return row['revision'], self.store.cipher.decrypt(credential['ciphertext']).decode(), json.loads(row['proxy_json'])

    def checked(self, organization, actor, account, revision, state, new_key=None):
        if state not in {'active', 'invalid', 'unavailable'}:
            raise ValueError('invalid_authorization_state')
        with self.store.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            row = self.row(db, organization, account)
            if row['revision'] != revision:
                raise Conflict('revision_conflict')
            if row['status'] == 'archived':
                raise Conflict('account_archived')
            version = row['credential_version']
            timestamp = now()
            if new_key is not None:
                if state != 'active':
                    # An unsuccessful rotation cannot invalidate the currently active key.
                    return self.project(row)
                version = db.execute('SELECT COALESCE(MAX(version),0)+1 FROM channel_account_credentials '
                                     'WHERE account_id=?', (account,)).fetchone()[0]
                db.execute('INSERT INTO channel_account_credentials VALUES (?,?,?,?)',
                           (account, version, self.store.cipher.encrypt(new_key.encode()), timestamp))
            db.execute('UPDATE channel_accounts SET authorization_state=?,credential_version=?,revision=revision+1,'
                       'updated_at=? WHERE id=?', (state, version, timestamp, account))
            db.execute('INSERT INTO channel_account_audit VALUES (NULL,?,?,?,?,?,?)',
                       (organization, account, actor, 'credential_rotated' if new_key else 'refreshed',
                        revision + 1, timestamp))
            return self.project(self.row(db, organization, account))

    def list(self, organization, *, search='', status='', offset=0, limit=50):
        if status not in {'', 'active', 'disabled', 'archived'} or not 1 <= limit <= 100 or offset < 0:
            raise ValueError('invalid_filter')
        with self.store.connect() as db:
            where = 'organization_id=? AND instr(name,?)>0 AND status ' + ('=?' if status else "!='archived'")
            args = [organization, search] + ([status] if status else [])
            total = db.execute('SELECT COUNT(*) FROM channel_accounts WHERE ' + where, args).fetchone()[0]
            rows = db.execute('SELECT * FROM channel_accounts WHERE ' + where +
                              ' ORDER BY created_at,id LIMIT ? OFFSET ?', [*args, limit, offset]).fetchall()
            return {'schema_version': 1, 'items': [self.project(row) for row in rows], 'total': total}

    def create(self, organization, actor, operation, value, *, validate_proxy=None):
        if not isinstance(operation, str) or not 1 <= len(operation) <= 128:
            raise ValueError('idempotency_key_required')
        if set(value) - {'name', 'api_url', 'api_key', 'proxy', 'status', 'default', 'registration', 'quota_subject_id'}:
            raise ValueError('invalid_account')
        key = value.get('api_key')
        if not isinstance(key, str) or not 16 <= len(key) <= 4096 or any(c.isspace() for c in key):
            raise ValueError('invalid_key')
        # Execution and control APIs have distinct fixed, reviewed origins.
        if value.get('api_url') != 'https://queue.fal.run':
            raise ValueError('unsupported_api_url')
        proxy = proxy_reference(value.get('proxy'))
        status = value.get('status', 'active')
        default = value.get('default', False)
        name = value.get('name', '').strip() if isinstance(value.get('name', ''), str) else ''
        if status not in {'active', 'disabled'} or type(default) is not bool or len(name) > 200:
            raise ValueError('invalid_account')
        registration = self.registration(value.get('registration', {}))
        canonical = json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode()
        # Keyed digest prevents a reusable credential fingerprint being stored in operation records.
        digest = hmac.new(self.store.operation_key, canonical, hashlib.sha256).hexdigest()
        account = str(uuid.uuid4())
        subject = value.get('quota_subject_id') or account
        if not isinstance(subject, str) or not 1 <= len(subject) <= 128:
            raise ValueError('invalid_quota_subject')
        timestamp = now()
        with self.store.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            prior = db.execute('SELECT * FROM channel_account_operations WHERE organization_id=? AND operation_id=?',
                               (organization, operation)).fetchone()
            if prior:
                if not hmac.compare_digest(prior['digest'], digest):
                    raise Conflict('idempotency_conflict')
                return self.project(self.row(db, organization, prior['account_id']))
            if validate_proxy is not None:
                validate_proxy(proxy)
            if default:
                db.execute('UPDATE channel_accounts SET is_default=0,revision=revision+1,updated_at=? '
                           'WHERE organization_id=? AND is_default=1', (timestamp, organization))
            db.execute('INSERT INTO channel_accounts VALUES (?,?,?,?,?,?,?,?,?,1,1,?,?,?)',
                       (account, organization, name or 'fal.ai 账号', value['api_url'], json.dumps(proxy),
                        status, int(default), subject, json.dumps(registration), 'pending_verification', timestamp, timestamp))
            db.execute('INSERT INTO channel_account_credentials VALUES (?,1,?,?)',
                       (account, self.store.cipher.encrypt(key.encode()), timestamp))
            db.execute('INSERT INTO channel_account_operations VALUES (?,?,?,?)', (organization, operation, digest, account))
            db.execute('INSERT INTO channel_account_audit VALUES (NULL,?,?,?,?,1,?)',
                       (organization, account, actor, 'created', timestamp))
            return self.project(self.row(db, organization, account))

    def patch(self, organization, actor, account, revision, value, *, archive=False):
        if type(revision) is not int or revision < 1:
            raise ValueError('expected_revision_required')
        if set(value) - {'name', 'status', 'default', 'registration', 'proxy'}:
            raise ValueError('invalid_account')
        with self.store.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            row = self.row(db, organization, account)
            if row['revision'] != revision:
                raise Conflict('revision_conflict')
            if row['status'] == 'archived':
                raise Conflict('account_archived')
            name = value.get('name', row['name'])
            status = 'archived' if archive else value.get('status', row['status'])
            default = False if archive else value.get('default', bool(row['is_default']))
            if not isinstance(name, str) or not name.strip() or len(name) > 200 or type(default) is not bool:
                raise ValueError('invalid_account')
            if status not in ({'archived'} if archive else {'active', 'disabled'}):
                raise ValueError('invalid_status')
            registration = self.registration(value.get('registration', json.loads(row['registration_json'])))
            proxy = proxy_reference(value.get('proxy', json.loads(row['proxy_json'])))
            authorization = row['authorization_state'] if proxy == json.loads(row['proxy_json']) else 'pending_verification'
            timestamp = now()
            if default:
                db.execute('UPDATE channel_accounts SET is_default=0,revision=revision+1,updated_at=? '
                           'WHERE organization_id=? AND id!=? AND is_default=1', (timestamp, organization, account))
            db.execute('UPDATE channel_accounts SET name=?,status=?,is_default=?,registration_json=?,proxy_json=?,authorization_state=?,revision=revision+1,'
                       'updated_at=? WHERE id=?', (name.strip(), status, int(default), json.dumps(registration),
                       json.dumps(proxy), authorization, timestamp, account))
            db.execute('INSERT INTO channel_account_audit VALUES (NULL,?,?,?,?,?,?)',
                       (organization, account, actor, 'archived' if archive else 'updated', revision + 1, timestamp))
            return self.project(self.row(db, organization, account))
