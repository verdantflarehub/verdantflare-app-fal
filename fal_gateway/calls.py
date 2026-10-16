"""Durable account-bound calls and shared USD reservations."""
from __future__ import annotations

import hashlib
import json
import uuid
from decimal import Decimal, InvalidOperation
from datetime import datetime, UTC

from .accounts import Accounts
from .store import Conflict, now


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
            ''')
            columns = {row['name'] for row in db.execute('PRAGMA table_info(channel_calls)')}
            if 'finished_at' not in columns:
                db.execute('ALTER TABLE channel_calls ADD COLUMN finished_at TEXT')
            if 'reservation_unknown' not in columns:
                db.execute('ALTER TABLE channel_calls ADD COLUMN reservation_unknown INTEGER NOT NULL DEFAULT 0')

    @staticmethod
    def project(row):
        row = dict(row)
        elapsed = None
        if row.get('finished_at'):
            elapsed = max(0, int((datetime.fromisoformat(row['finished_at']) -
                                  datetime.fromisoformat(row['created_at'])).total_seconds()))
        return {'schema_version': 1, 'call_id': row['id'], 'task_id': row['task_id'],
                'account_id': row['account_id'], 'model_id': row['model_id'],
                'account_name': row.get('account_name'), 'account_status': row.get('account_status'),
                'media': 'video', 'operation': 'generate', 'duration_seconds': elapsed,
                'duration_source': 'local_elapsed' if elapsed is not None else 'unknown',
                'state': row['state'], 'accepted': None if row['accepted'] is None else bool(row['accepted']),
                'revision': row['revision'], 'reason_code': None,
                'budget': {'amount': row['budget_amount'], 'unit': 'USD', 'source': 'unknown'},
                'settlement': {'amount': row['settlement_amount'], 'unit': 'USD',
                               'source': row['settlement_source']},
                'created_at': row['created_at'], 'updated_at': row['updated_at']}

    def get(self, organization, call_id, caller=None):
        with self.store.connect() as db:
            row = db.execute('SELECT * FROM channel_calls WHERE organization_id=? AND id=?',
                             (organization, call_id)).fetchone()
        if row is None or (caller is not None and row['caller_id'] != caller):
            raise KeyError('call_not_found')
        return dict(row)

    def by_operation(self, organization, caller, operation):
        with self.store.connect() as db:
            row = db.execute('SELECT * FROM channel_calls WHERE organization_id=? AND caller_id=? AND operation_id=?',
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
                 'a.id=c.account_id AND a.organization_id=c.organization_id WHERE ' + ' AND '.join(where))
        with self.store.connect() as db:
            # Both the page and full-filter summary come from the same read snapshot.
            db.execute('BEGIN')
            amounts = db.execute('SELECT budget_amount,settlement_amount' + query, values).fetchall()
            rows = db.execute('SELECT c.*,a.name AS account_name,a.status AS account_status' + query +
                              ' ORDER BY c.created_at DESC,c.id LIMIT ? OFFSET ?',
                              [*values, limit, offset]).fetchall()
            account_options = db.execute('SELECT id AS account_id,name,status FROM channel_accounts '
                                         'WHERE organization_id=? ORDER BY name,id', (organization,)).fetchall()
        def total(field):
            known = [Decimal(row[field]) for row in amounts if row[field] is not None]
            return {'amount': format(sum(known, Decimal(0)), 'f') if known or not amounts else None,
                    'unit': 'USD', 'unknown_count': len(amounts) - len(known)}
        return {'schema_version': 1, 'items': [self.project(row) for row in rows], 'total': len(amounts),
                'accounts': [dict(row) for row in account_options],
                'summary': {'task_count': len(amounts), 'budget': total('budget_amount'),
                            'settlement': total('settlement_amount')}}

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
            previous = db.execute('SELECT * FROM channel_calls WHERE organization_id=? AND caller_id=? AND operation_id=?',
                                  (organization, caller, operation)).fetchone()
            if previous:
                if previous['digest'] != digest:
                    raise Conflict('idempotency_conflict')
                return dict(previous), False
            if db.execute('SELECT 1 FROM submissions WHERE organization_id=? AND operation_id=?',
                          (organization, operation)).fetchone():
                raise Conflict('legacy_call_requires_recovery')
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
            if json.loads(account['proxy_json']) != {'mode': 'direct'}:
                raise Conflict('proxy_unavailable')
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
            charges = db.execute('SELECT reservation_amount,settlement_amount,reservation_unknown FROM channel_calls '
                                 'WHERE organization_id=? AND quota_subject_id=?',
                                 (organization, account['quota_subject_id'])).fetchall()
            if limits and any(row['reservation_unknown'] and row['settlement_amount'] is None for row in charges):
                raise Conflict('historical_cost_unknown')
            committed = sum((Decimal(row['settlement_amount']) if row['settlement_amount'] is not None
                             else Decimal(row['reservation_amount']) for row in charges), Decimal(0))
            if limits and committed + Decimal(ceiling) > min(limits):
                raise Conflict('budget_exceeded')
            call_id, timestamp = str(uuid.uuid4()), now()
            db.execute('''INSERT INTO channel_calls
                (id,organization_id,caller_id,operation_id,digest,task_id,account_id,credential_version,
                 quota_subject_id,model_id,state,reservation_amount,created_at,updated_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,'acceptance_unknown',?,?,?)''',
                (call_id, organization, caller, operation, digest, task, account['id'],
                 account['credential_version'], account['quota_subject_id'], model, ceiling, timestamp, timestamp))
            return dict(db.execute('SELECT * FROM channel_calls WHERE id=?', (call_id,)).fetchone()), True

    def credential(self, row):
        # Recovery uses the pinned version even after account rotation, disabling or archival.
        with self.store.connect() as db:
            key = db.execute('SELECT ciphertext FROM channel_account_credentials WHERE account_id=? AND version=?',
                             (row['account_id'], row['credential_version'])).fetchone()
        if key is None:
            raise Conflict('recovery_credentials_unavailable')
        return self.store.cipher.decrypt(key['ciphertext']).decode()

    def accepted(self, call_id, remote):
        with self.store.connect() as db:
            db.execute("UPDATE channel_calls SET provider_request_id=?,state='queued',accepted=1,"
                       "revision=revision+1,updated_at=? WHERE id=? AND accepted IS NULL",
                       (remote, now(), call_id))

    def rejected(self, call_id):
        with self.store.connect() as db:
            db.execute("UPDATE channel_calls SET state='rejected',accepted=0,reservation_amount='0',reservation_unknown=0,"
                       "revision=revision+1,updated_at=?,finished_at=? WHERE id=? AND accepted IS NULL",
                       (timestamp := now(), timestamp, call_id))

    def observed(self, call_id, state):
        if state not in {'in_queue', 'in_progress', 'completed', 'failed'}:
            raise ValueError('invalid_call_state')
        with self.store.connect() as db:
            timestamp = now()
            db.execute("UPDATE channel_calls SET state=?,updated_at=?,finished_at=?,revision=revision+1 "
                       "WHERE id=? AND accepted=1 AND state NOT IN ('completed','failed') AND state!=?",
                       (state, timestamp, timestamp if state in {'completed', 'failed'} else None, call_id, state))
