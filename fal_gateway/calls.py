"""Durable account-bound calls and shared USD reservations."""
from __future__ import annotations

import hashlib
import hmac
import json
import uuid
from decimal import Decimal, InvalidOperation
from datetime import datetime, UTC

from .accounts import Accounts
from .store import Conflict, now
from .proxy import proxy_reference
from .billing import business_estimate, estimate, input_digest


ESTIMATE_FIELDS = ('e.amount AS business_estimate_amount,e.source AS business_estimate_source,'
                   'e.evidence_json AS business_estimate_evidence,e.observed_at AS business_estimate_observed_at')
ESTIMATE_JOIN = (' LEFT JOIN channel_call_estimates e ON e.call_id=c.id '
                'AND e.organization_id=c.organization_id ')


def occupancy(row):
    row = dict(row)
    if row['settlement_amount'] is not None:
        return {'amount': row['settlement_amount'], 'unit': 'USD', 'source': row['settlement_source']}
    estimated = row.get('business_estimate_amount')
    if row.get('accepted') != 1 or row.get('state') != 'completed':
        estimated = None
    reserved = None if row.get('reservation_unknown') else row['reservation_amount']
    if estimated is not None and (reserved is None or Decimal(estimated) > Decimal(reserved)):
        return {'amount': estimated, 'unit': 'USD', 'source': 'business_estimate'}
    return {'amount': reserved, 'unit': 'USD', 'source': 'budget_limit' if reserved is not None else 'unknown'}


def amount(value):
    if not isinstance(value, str) or not 1 <= len(value) <= 32:
        raise ValueError('invalid_amount')
    try:
        number = Decimal(value)
        if not number.is_finite() or number < 0 or number > Decimal('1e15') or number.as_tuple().exponent < -8:
            raise ValueError('invalid_amount')
    except InvalidOperation:
        raise ValueError('invalid_amount') from None
    return '0' if number == 0 else format(number, 'f')


class Calls:
    def __init__(self, store):
        self.store = store
        Accounts(store)
        with store.connect() as db:
            db.executescript('''
                CREATE TABLE IF NOT EXISTS channel_calls (
                  id TEXT PRIMARY KEY, organization_id TEXT NOT NULL,
                  caller_id TEXT NOT NULL, operation_id TEXT NOT NULL, digest TEXT NOT NULL,
                  task_id TEXT NOT NULL, account_id TEXT NOT NULL, credential_version INTEGER NOT NULL,
                  quota_subject_id TEXT NOT NULL, model_id TEXT NOT NULL,
                  provider_request_id TEXT, state TEXT NOT NULL, accepted INTEGER,
                  budget_amount TEXT, reservation_amount TEXT NOT NULL, settlement_amount TEXT,
                  settlement_source TEXT NOT NULL DEFAULT 'unknown',
                  created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
                  revision INTEGER NOT NULL DEFAULT 1,
                  UNIQUE(organization_id, caller_id, operation_id)
                );
                CREATE INDEX IF NOT EXISTS channel_calls_quota
                  ON channel_calls(organization_id, quota_subject_id);
                CREATE TABLE IF NOT EXISTS channel_call_settlements (
                  call_id TEXT PRIMARY KEY, organization_id TEXT NOT NULL,
                  actor TEXT NOT NULL, amount TEXT NOT NULL, source TEXT NOT NULL,
                  evidence_digest TEXT NOT NULL, evidence_json TEXT NOT NULL,
                  event_count INTEGER NOT NULL, observed_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS channel_call_estimates (
                  call_id TEXT PRIMARY KEY, organization_id TEXT NOT NULL,
                  actor TEXT NOT NULL, amount TEXT NOT NULL, source TEXT NOT NULL,
                  original_digest TEXT NOT NULL, evidence_digest TEXT NOT NULL,
                  evidence_json TEXT NOT NULL, observed_at TEXT NOT NULL
                );
            ''')
            columns = {row['name'] for row in db.execute('PRAGMA table_info(channel_calls)')}
            if 'finished_at' not in columns:
                db.execute('ALTER TABLE channel_calls ADD COLUMN finished_at TEXT')
            if 'reservation_unknown' not in columns:
                db.execute('ALTER TABLE channel_calls ADD COLUMN reservation_unknown INTEGER NOT NULL DEFAULT 0')
            if 'proxy_json' not in columns:
                # Pre-proxy native calls were explicit-direct only. Legacy calls
                # retain their original submissions/transport recovery path.
                db.execute("ALTER TABLE channel_calls ADD COLUMN proxy_json TEXT NOT NULL DEFAULT '{\"mode\":\"direct\"}'")
            if 'billing_json' not in columns:
                db.execute("ALTER TABLE channel_calls ADD COLUMN billing_json TEXT")
            if 'error_json' not in columns:
                db.execute("ALTER TABLE channel_calls ADD COLUMN error_json TEXT")

    @staticmethod
    def project(row):
        row = dict(row)
        # finished_at is when we observed completion, not the supplier's finish
        # time. Delayed polling must never become a fabricated generation duration.
        elapsed = None
        billing = json.loads(row['billing_json']) if row.get('billing_json') else None
        return {'schema_version': 1, 'call_id': row['id'], 'task_id': row['task_id'],
                'account_id': row['account_id'], 'model_id': row['model_id'],
                'account_name': row.get('account_name'), 'account_status': row.get('account_status'),
                'media': 'video', 'operation': 'generate', 'duration_seconds': elapsed,
                'duration_source': 'local_elapsed' if elapsed is not None else 'unknown',
                'state': row['state'], 'accepted': None if row['accepted'] is None else bool(row['accepted']),
                'revision': row['revision'],
                'reason_code': json.loads(row['error_json'])['code'] if row.get('error_json') else None,
                'error': json.loads(row['error_json']) if row.get('error_json') else None,
                'budget': {'amount': row['budget_amount'], 'unit': 'USD',
                           'source': billing['estimate']['source'] if billing else 'unknown'},
                'reservation': {'amount': None if row.get('reservation_unknown') else row['reservation_amount'],
                                'unit': 'USD', 'source': 'unknown' if row.get('reservation_unknown') else 'budget_limit'},
                'pricing': billing,
                'settlement': {'amount': row['settlement_amount'], 'unit': 'USD',
                               'source': row['settlement_source']},
                'business_estimate': {'amount': row.get('business_estimate_amount'), 'unit': 'USD',
                    'source': row.get('business_estimate_source') or 'unknown',
                    'observed_at': row.get('business_estimate_observed_at'),
                    'basis': json.loads(row['business_estimate_evidence']) if row.get('business_estimate_evidence') else None},
                'budget_occupancy': occupancy(row),
                'created_at': row['created_at'], 'updated_at': row['updated_at']}

    def get(self, organization, call_id, caller=None):
        with self.store.connect() as db:
            row = db.execute('SELECT c.*,' + ESTIMATE_FIELDS + ' FROM channel_calls c' + ESTIMATE_JOIN +
                             'WHERE c.organization_id=? AND c.id=?',
                             (organization, call_id)).fetchone()
        if row is None or (caller is not None and row['caller_id'] != caller):
            raise KeyError('call_not_found')
        return dict(row)

    def by_operation(self, organization, caller, operation):
        with self.store.connect() as db:
            row = db.execute('SELECT c.*,' + ESTIMATE_FIELDS + ' FROM channel_calls c' + ESTIMATE_JOIN +
                             'WHERE c.organization_id=? AND c.caller_id=? AND c.operation_id=?',
                             (organization, caller, operation)).fetchone()
        if row is None:
            raise KeyError('call_not_found')
        return self.project(row)

    def list(self, organization, *, account=None, state=None, search='', offset=0, limit=50,
             media=None, created_from=None, created_before=None):
        if type(offset) is not int or offset < 0 or type(limit) is not int or not 1 <= limit <= 100:
            raise ValueError('invalid_pagination')
        if state is not None and state not in {'acceptance_unknown', 'queued', 'in_queue', 'in_progress',
                                               'completed', 'failed', 'rejected'}:
            raise ValueError('invalid_state')
        if media not in {None, '', 'image', 'video', 'music', 'speech'}:
            raise ValueError('invalid_media')
        bounds = []
        for value in (created_from, created_before):
            if not value:
                bounds.append(None)
                continue
            try:
                timestamp = datetime.fromisoformat(value)
                if timestamp.tzinfo is None:
                    raise ValueError('timezone_required')
                bounds.append(timestamp.astimezone(UTC).isoformat())
            except (ValueError, TypeError):
                raise ValueError('invalid_date_filter') from None
        if all(bounds) and bounds[0] >= bounds[1]:
            raise ValueError('invalid_date_range')
        where, values = ['c.organization_id=?'], [organization]
        if account:
            where.append('c.account_id=?')
            values.append(account)
        if state:
            where.append('c.state=?')
            values.append(state)
        if search:
            where.append('(instr(c.task_id,?)>0 OR instr(c.model_id,?)>0 OR instr(c.id,?)>0 OR instr(a.name,?)>0)')
            values.extend([search] * 4)
        if media and media != 'video':
            where.append('0=1')
        for bound, comparison in zip(bounds, ('>=', '<')):
            if bound:
                where.append('c.created_at' + comparison + '?')
                values.append(bound)
        query = (' FROM channel_calls c LEFT JOIN channel_accounts a ON '
                 'a.id=c.account_id AND a.organization_id=c.organization_id' + ESTIMATE_JOIN + 'WHERE ' + ' AND '.join(where))
        with self.store.connect() as db:
            # Both the page and full-filter summary come from the same read snapshot.
            db.execute('BEGIN')
            amounts = db.execute('SELECT c.*,' + ESTIMATE_FIELDS + query, values).fetchall()
            rows = db.execute('SELECT c.*,' + ESTIMATE_FIELDS + ',a.name AS account_name,a.status AS account_status' + query +
                              ' ORDER BY c.created_at DESC,c.id LIMIT ? OFFSET ?',
                              [*values, limit, offset]).fetchall()
            account_options = db.execute('SELECT id AS account_id,name,status FROM channel_accounts '
                                         'WHERE organization_id=? ORDER BY name,id', (organization,)).fetchall()
        def total(field):
            known = [Decimal(row[field]) for row in amounts if row[field] is not None]
            return {'amount': format(sum(known, Decimal(0)), 'f') if known or not amounts else None,
                    'unit': 'USD', 'unknown_count': len(amounts) - len(known)}
        effective = [occupancy(row) for row in amounts]
        known_occupancy = [Decimal(value['amount']) for value in effective if value['amount'] is not None]
        occupancy_total = {'amount': format(sum(known_occupancy, Decimal(0)), 'f') if known_occupancy or not amounts else None,
                           'unit': 'USD', 'unknown_count': len(amounts) - len(known_occupancy),
                           'estimated_count': sum(value['source'] == 'business_estimate' for value in effective)}
        return {'schema_version': 1, 'items': [self.project(row) for row in rows], 'total': len(amounts),
                'accounts': [dict(row) for row in account_options],
                'summary': {'task_count': len(amounts), 'budget': total('budget_amount'),
                            'settlement': total('settlement_amount'), 'budget_occupancy': occupancy_total}}

    @staticmethod
    def _admit(db, organization, account_id, ceiling):
        if account_id:
            account = Accounts.row(db, organization, account_id)
        else:
            account = db.execute('SELECT * FROM channel_accounts WHERE organization_id=? AND is_default=1',
                                 (organization,)).fetchone()
        if account is None:
            raise Conflict('account_required')
        if account['status'] != 'active':
            raise Conflict('account_disabled')
        if account['authorization_state'] != 'active':
            raise Conflict('authorization_not_verified')
        proxy = proxy_reference(json.loads(account['proxy_json']))
        # All keys sharing a quota subject share the same locked accounting boundary.
        peers = db.execute('SELECT registration_json FROM channel_accounts WHERE organization_id=? AND quota_subject_id=?',
                           (organization, account['quota_subject_id'])).fetchall()
        limits = [Decimal(data['budget_limit']) for peer in peers
                  if (data := json.loads(peer['registration_json'])).get('budget_limit') is not None]
        daily_limits = [data['daily_limit'] for peer in peers
                        if (data := json.loads(peer['registration_json'])).get('daily_limit') is not None]
        if daily_limits:
            count = db.execute('SELECT COUNT(*) FROM channel_calls WHERE organization_id=? AND '
                               'quota_subject_id=? AND created_at>=?',
                               (organization, account['quota_subject_id'], datetime.now(UTC).date().isoformat())).fetchone()[0]
            if count >= min(daily_limits):
                raise Conflict('daily_limit_reached')
        charges = db.execute('SELECT c.*,' + ESTIMATE_FIELDS + ' FROM channel_calls c' + ESTIMATE_JOIN +
                             'WHERE c.organization_id=? AND c.quota_subject_id=?',
                             (organization, account['quota_subject_id'])).fetchall()
        effective = [occupancy(row) for row in charges]
        if limits and any(value['amount'] is None for value in effective):
            raise Conflict('historical_cost_unknown')
        committed = sum((Decimal(value['amount']) for value in effective if value['amount'] is not None), Decimal(0))
        if limits and committed + Decimal(ceiling) > min(limits):
            raise Conflict('budget_exceeded')
        return account, proxy

    def reserve(self, organization, caller, operation, task, model, payload, account_id, ceiling):
        # A ceiling is a reservation, never a fabricated preflight estimate or settlement.
        if ceiling is None:
            raise Conflict('cost_unknown')
        ceiling = amount(ceiling)
        if Decimal(ceiling) <= 0:
            raise Conflict('cost_unknown')
        canonical = json.dumps([task, model, payload, account_id, ceiling],
                               sort_keys=True, separators=(',', ':'), ensure_ascii=False)
        digest = hashlib.sha256(canonical.encode()).hexdigest()
        with self.store.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            previous = db.execute('SELECT c.*,' + ESTIMATE_FIELDS + ' FROM channel_calls c' + ESTIMATE_JOIN +
                                  'WHERE c.organization_id=? AND c.caller_id=? AND c.operation_id=?',
                                  (organization, caller, operation)).fetchone()
            if previous:
                if previous['digest'] != digest:
                    raise Conflict('idempotency_conflict')
                return dict(previous), False
            if db.execute('SELECT 1 FROM submissions WHERE organization_id=? AND operation_id=?',
                          (organization, operation)).fetchone():
                raise Conflict('legacy_call_requires_recovery')
            billing = estimate(model, payload)
            estimated = billing['estimate']['amount']
            if estimated is not None and Decimal(estimated) > Decimal(ceiling):
                raise Conflict('budget_below_estimate')
            account, proxy = self._admit(db, organization, account_id, ceiling)
            call_id, timestamp = str(uuid.uuid4()), now()
            db.execute('''INSERT INTO channel_calls
                (id,organization_id,caller_id,operation_id,digest,task_id,account_id,credential_version,
                 quota_subject_id,model_id,state,reservation_amount,created_at,updated_at,proxy_json,budget_amount,billing_json)
                VALUES (?,?,?,?,?,?,?,?,?,?,'acceptance_unknown',?,?,?,?,?,?)''',
                (call_id, organization, caller, operation, digest, task, account['id'],
                 account['credential_version'], account['quota_subject_id'], model, ceiling, timestamp, timestamp,
                 json.dumps(proxy), estimated, json.dumps(billing)))
            return dict(db.execute('SELECT * FROM channel_calls WHERE id=?', (call_id,)).fetchone()), True

    def preflight(self, organization, model, payload, account_id, ceiling):
        billing = estimate(model, payload)
        result = {'schema_version': 1, 'channel_id': 'fal', 'model_id': model,
                  'can_submit': False, 'blocked_reason': None, 'pricing': billing,
                  'observed_at': now(), 'account_id': None,
                  'admission_scope': 'local_account_ledger; rechecked_on_create'}
        try:
            if ceiling is None:
                raise Conflict('cost_unknown')
            ceiling = amount(ceiling)
            if Decimal(ceiling) <= 0:
                raise Conflict('cost_unknown')
            estimated = billing['estimate']['amount']
            if estimated is not None and Decimal(estimated) > Decimal(ceiling):
                raise Conflict('budget_below_estimate')
            with self.store.connect() as db:
                db.execute('PRAGMA query_only=ON')
                db.execute('BEGIN')
                account, _ = self._admit(db, organization, account_id, ceiling)
                result.update(can_submit=True, account_id=account['id'])
        except Conflict as error:
            result['blocked_reason'] = str(error)
        return result

    def credential(self, row):
        # Recovery uses the pinned version even after account rotation, disabling or archival.
        with self.store.connect() as db:
            key = db.execute('SELECT ciphertext FROM channel_account_credentials WHERE account_id=? AND version=?',
                             (row['account_id'], row['credential_version'])).fetchone()
        if key is None:
            raise Conflict('recovery_credentials_unavailable')
        return self.store.cipher.decrypt(key['ciphertext']).decode()

    def estimate_business(self, original, actor, payload):
        if original['accepted'] != 1 or original['state'] != 'completed' or not original['provider_request_id']:
            raise Conflict('business_estimate_requires_completed_call')
        if original['settlement_amount'] is not None:
            raise Conflict('business_estimate_not_required')
        if original['caller_id'] == 'legacy-video-mcp':
            canonical = json.dumps(payload, sort_keys=True, separators=(',', ':'), ensure_ascii=False)
            expected = hashlib.sha256((original['model_id'] + '\n' + canonical).encode()).hexdigest()
            verified = hmac.compare_digest(expected, original['digest'])
        else:
            billing = json.loads(original['billing_json']) if original.get('billing_json') else {}
            verified = hmac.compare_digest(input_digest(original['model_id'], payload), billing.get('input_digest', ''))
        if not verified:
            raise Conflict('business_estimate_input_mismatch')
        with self.store.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            current = db.execute('SELECT c.*,' + ESTIMATE_FIELDS + ' FROM channel_calls c' + ESTIMATE_JOIN +
                                 'WHERE c.organization_id=? AND c.id=?',
                                 (original['organization_id'], original['id'])).fetchone()
            if current is None:
                raise KeyError('call_not_found')
            if any(current[field] != original[field] for field in ('revision', 'digest', 'provider_request_id',
                    'model_id', 'accepted', 'state', 'settlement_amount')):
                raise Conflict('revision_conflict')
            existing = db.execute('SELECT * FROM channel_call_estimates WHERE call_id=?', (original['id'],)).fetchone()
            if existing:
                saved = json.loads(existing['evidence_json'])
                if (existing['original_digest'] != original['digest']
                        or saved.get('input_digest') != input_digest(original['model_id'], payload)
                        or saved.get('amount') != existing['amount'] or saved.get('source') != existing['source']
                        or hashlib.sha256(existing['evidence_json'].encode()).hexdigest() != existing['evidence_digest']):
                    raise Conflict('business_estimate_conflict')
            else:
                evidence = business_estimate(original['model_id'], payload)
                db.execute('INSERT INTO channel_call_estimates VALUES (?,?,?,?,?,?,?,?,?)',
                           (original['id'], original['organization_id'], actor, evidence['amount'], evidence['source'],
                            original['digest'], evidence['evidence_digest'], evidence['evidence_json'], evidence['observed_at']))
                db.execute('UPDATE channel_calls SET revision=revision+1,updated_at=? WHERE id=?', (now(), original['id']))
            row = db.execute('SELECT c.*,' + ESTIMATE_FIELDS + ' FROM channel_calls c' + ESTIMATE_JOIN +
                             'WHERE c.id=?', (original['id'],)).fetchone()
            return self.project(row)

    def settle_bill(self, original, actor, bill):
        with self.store.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            current = db.execute('SELECT c.*,' + ESTIMATE_FIELDS + ' FROM channel_calls c' + ESTIMATE_JOIN +
                                 'WHERE c.organization_id=? AND c.id=?',
                                 (original['organization_id'], original['id'])).fetchone()
            if current is None:
                raise KeyError('call_not_found')
            existing = db.execute('SELECT * FROM channel_call_settlements WHERE call_id=?', (original['id'],)).fetchone()
            if existing:
                if (existing['evidence_digest'] != bill['evidence_digest'] or existing['amount'] != bill['amount']
                        or current['settlement_amount'] != existing['amount'] or current['settlement_source'] != existing['source']):
                    raise Conflict('settlement_conflict')
                return self.project(current)
            if current['settlement_amount'] is not None:
                raise Conflict('settlement_conflict')
            if (current['revision'] != original['revision']
                    or current['provider_request_id'] != original['provider_request_id']
                    or current['model_id'] != original['model_id']):
                raise Conflict('revision_conflict')
            db.execute('INSERT INTO channel_call_settlements VALUES (?,?,?,?,?,?,?,?,?)',
                       (original['id'], original['organization_id'], actor, bill['amount'], bill['source'],
                        bill['evidence_digest'], bill['evidence_json'], bill['event_count'], bill['observed_at']))
            db.execute('UPDATE channel_calls SET settlement_amount=?,settlement_source=?,revision=revision+1,updated_at=? WHERE id=?',
                       (bill['amount'], bill['source'], now(), original['id']))
            return self.project(db.execute('SELECT c.*,' + ESTIMATE_FIELDS + ' FROM channel_calls c' + ESTIMATE_JOIN +
                                           'WHERE c.id=?', (original['id'],)).fetchone())

    def accepted(self, call_id, remote):
        with self.store.connect() as db:
            db.execute("UPDATE channel_calls SET provider_request_id=?,state='queued',accepted=1,"
                       "revision=revision+1,updated_at=? WHERE id=? AND accepted IS NULL",
                       (remote, now(), call_id))

    def rejected(self, call_id, error=None):
        with self.store.connect() as db:
            db.execute("UPDATE channel_calls SET state='rejected',accepted=0,reservation_amount='0',reservation_unknown=0,"
                       "revision=revision+1,updated_at=?,finished_at=?,error_json=? WHERE id=? AND accepted IS NULL",
                       (timestamp := now(), timestamp, json.dumps(error) if error else None, call_id))

    def observed(self, call_id, state, error=None):
        if state not in {'in_queue', 'in_progress', 'completed', 'failed'}:
            raise ValueError('invalid_call_state')
        with self.store.connect() as db:
            timestamp = now()
            db.execute("UPDATE channel_calls SET state=?,updated_at=?,finished_at=?,error_json=?,revision=revision+1 "
                       "WHERE id=? AND accepted=1 AND state NOT IN ('completed','failed') AND state!=?",
                       (state, timestamp, timestamp if state in {'completed', 'failed'} else None,
                        json.dumps(error) if error else None, call_id, state))
