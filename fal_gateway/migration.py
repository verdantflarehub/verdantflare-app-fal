"""Atomic cutover from organization credentials to durable channel accounts."""
import json
import hashlib
import hmac
import os
import sqlite3
import uuid
from contextlib import closing
from urllib.parse import urlsplit, unquote

from .store import now
from .proxy import proxy_reference


LEGACY_TABLES = ('organizations', 'credentials', 'submissions')
CHANNEL_TABLES = ('channel_accounts', 'channel_account_credentials',
                  'channel_account_operations', 'channel_account_audit',
                  'channel_calls', 'channel_legacy_accounts')


def fingerprint(db, store, tables):
    """Stable keyed digest; neither plaintext nor encrypted credentials are logged."""
    digest = hmac.new(store.operation_key, digestmod=hashlib.sha256)
    for table in tables:
        rows = []
        for row in db.execute('SELECT * FROM ' + table):
            values = [value.hex() if isinstance(value, bytes) else value for value in row]
            rows.append(json.dumps(values, ensure_ascii=False, separators=(',', ':')))
        digest.update(json.dumps([table, sorted(rows)], ensure_ascii=False).encode())
    return digest.hexdigest()


def retry_required(db, store):
    if not db.execute("SELECT 1 FROM channel_metadata WHERE name='accounts_cutover'").fetchone():
        return False
    checkpoint = dict(db.execute("SELECT name,value FROM channel_metadata WHERE name IN "
                                 "('legacy_source_digest','legacy_target_digest')"))
    if len(checkpoint) != 2:
        raise ValueError('legacy_migration_checkpoint_missing')
    if hmac.compare_digest(checkpoint['legacy_source_digest'], fingerprint(db, store, LEGACY_TABLES)):
        return False
    if not hmac.compare_digest(checkpoint['legacy_target_digest'], fingerprint(db, store, CHANNEL_TABLES)):
        raise ValueError('legacy_migration_reconciliation_required')
    return True


def resolve_legacy_proxies(store, network, legacy_url, reference):
    """Explicit operator mapping, preserving the old proxy host/port and auth."""
    with store.connect() as db:
        if db.execute("SELECT 1 FROM channel_metadata WHERE name='accounts_cutover'").fetchone() and not retry_required(db, store):
            return {}
        organizations = [r[0] for r in db.execute('SELECT DISTINCT organization_id FROM credentials')]
    if not organizations or not legacy_url:
        return {}
    if network is None or reference is None:
        raise ValueError('legacy_proxy_mapping_required')
    reference = proxy_reference(reference)
    if reference['mode'] != 'proxy':
        raise ValueError('legacy_proxy_mapping_required')
    try:
        old = urlsplit(legacy_url)
        if old.scheme not in {'http', 'https', 'socks5', 'socks5h'} or not old.hostname or old.path not in {'', '/'} or old.query or old.fragment:
            raise ValueError()
        def endpoint(url):
            return (url.hostname, url.port or {'http': 80, 'https': 443, 'socks5': 1080, 'socks5h': 1080}[url.scheme],
                    unquote(url.username or ''), unquote(url.password or ''))
        expected = endpoint(old)
        for organization in organizations:
            candidate = urlsplit(network.resolve_url(organization, reference))
            if endpoint(candidate) != expected:
                raise ValueError()
    except Exception:
        raise ValueError('legacy_proxy_mapping_unverified') from None
    return {organization: dict(reference) for organization in organizations}


def backup_legacy(store):
    """Called under execution_owner, before cutover or accepting new requests."""
    with store.connect() as source:
        if source.execute("SELECT 1 FROM channel_metadata WHERE name='accounts_cutover'").fetchone() and not retry_required(source, store):
            return None
        if not any(source.execute('SELECT 1 FROM ' + table + ' LIMIT 1').fetchone() for table in LEGACY_TABLES):
            return None
        expected = fingerprint(source, store, LEGACY_TABLES)
        directory = store.path.parent / 'backups'
        if directory.is_symlink():
            raise ValueError('legacy_backup_unavailable')
        directory.mkdir(mode=0o700, exist_ok=True)
        destination = directory / ('channel-precutover-' + expected + '.sqlite3')

        def verify(path):
            if path.is_symlink() or not path.is_file():
                raise ValueError('legacy_backup_unavailable')
            with closing(sqlite3.connect(path.resolve().as_uri() + '?mode=ro', uri=True)) as check:
                if check.execute('PRAGMA integrity_check').fetchone()[0] != 'ok' or fingerprint(check, store, LEGACY_TABLES) != expected:
                    raise ValueError('legacy_backup_mismatch')

        if destination.exists():
            verify(destination)
            return destination
        temporary = directory / ('.precutover-' + uuid.uuid4().hex + '.sqlite3')
        try:
            with temporary.open('xb'):
                pass
            os.chmod(temporary, 0o600)
            with closing(sqlite3.connect(temporary)) as copy:
                source.backup(copy)
            verify(temporary)
            if fingerprint(source, store, LEGACY_TABLES) != expected:
                raise ValueError('legacy_backup_source_changed')
            with temporary.open('r+b') as saved:
                os.fsync(saved.fileno())
            try:
                os.link(temporary, destination)
            except FileExistsError:
                verify(destination)
            if os.name != 'nt':
                descriptor = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
                try:
                    os.fsync(descriptor)
                finally:
                    os.close(descriptor)
        finally:
            temporary.unlink(missing_ok=True)
        return destination


def migrate_legacy(store, *, legacy_proxy_configured=False, legacy_proxies=None):
    """Run after Accounts/Calls schema initialization and before accepting requests."""
    with store.connect() as db:
        db.execute('BEGIN IMMEDIATE')
        original_ids = {}
        if db.execute("SELECT 1 FROM channel_metadata WHERE name='accounts_cutover'").fetchone():
            if not retry_required(db, store):
                return
            original_ids = dict(db.execute('SELECT organization_id,account_id FROM channel_legacy_accounts'))
            # Only discard untouched migration projections, never native accounts or calls.
            for account in original_ids.values():
                db.execute('DELETE FROM channel_calls WHERE account_id=?', (account,))
                db.execute('DELETE FROM channel_account_audit WHERE account_id=?', (account,))
                db.execute('DELETE FROM channel_account_credentials WHERE account_id=?', (account,))
                db.execute('DELETE FROM channel_accounts WHERE id=?', (account,))
            db.execute('DELETE FROM channel_legacy_accounts')
            db.execute("DELETE FROM channel_metadata WHERE name IN "
                       "('accounts_cutover','legacy_source_digest','legacy_target_digest')")
        timestamp = now()
        organizations = db.execute('SELECT * FROM organizations ORDER BY organization_id').fetchall()
        legacy_proxies = legacy_proxies or {}
        if legacy_proxy_configured:
            expected = {row[0] for row in db.execute('SELECT DISTINCT organization_id FROM credentials')}
            if set(legacy_proxies) != expected:
                raise ValueError('legacy_proxy_mapping_required')
            if any(proxy_reference(value)['mode'] != 'proxy' for value in legacy_proxies.values()):
                raise ValueError('legacy_proxy_mapping_required')
        for org in organizations:
            organization = org['organization_id']
            credentials = db.execute('SELECT * FROM credentials WHERE organization_id=? ORDER BY version',
                                     (organization,)).fetchall()
            if not credentials:
                if db.execute('SELECT 1 FROM submissions WHERE organization_id=? LIMIT 1', (organization,)).fetchone():
                    raise ValueError('legacy_recovery_credential_missing')
                continue
            proxy = proxy_reference(legacy_proxies.get(organization, {'mode': 'direct'}))
            account = original_ids.get(organization) or str(uuid.uuid4())
            active = next((row for row in credentials if row['version'] == org['active_version']), None)
            current = active or credentials[-1]
            auth = current['state'] if current['state'] in {'active', 'invalid', 'unavailable', 'pending_verification'} else 'invalid'
            enabled = bool(active and org['enabled'])
            default = db.execute('SELECT 1 FROM channel_accounts WHERE organization_id=? AND is_default=1',
                                 (organization,)).fetchone() is None
            db.execute('''INSERT INTO channel_accounts
                (id,organization_id,name,api_url,proxy_json,status,is_default,quota_subject_id,registration_json,
                 revision,credential_version,authorization_state,created_at,updated_at)
                VALUES (?,?,?,?,?,?,?,?,?,1,?,?,?,?)''',
                (account, organization, 'fal.ai 原有账号', 'https://queue.fal.run', json.dumps(proxy),
                 'active' if enabled else 'disabled', int(default), account,
                 json.dumps({'daily_limit': org['daily_limit']}), current['version'], auth,
                 credentials[0]['created_at'], timestamp))
            for credential in credentials:
                db.execute('INSERT INTO channel_account_credentials VALUES (?,?,?,?)',
                           (account, credential['version'], credential['ciphertext'], credential['created_at']))
            db.execute('INSERT INTO channel_legacy_accounts VALUES (?,?,?)', (organization, account, timestamp))
            db.execute('INSERT INTO channel_account_audit VALUES (NULL,?,?,?,?,1,?)',
                       (organization, account, 'system:migration', 'legacy_imported', timestamp))
            for item in db.execute('SELECT * FROM submissions WHERE organization_id=?', (organization,)).fetchall():
                if item['credential_version'] not in {row['version'] for row in credentials}:
                    raise ValueError('legacy_recovery_credential_missing')
                accepted = 0 if item['state'] == 'rejected' else 1 if item['provider_request_id'] else None
                state = 'rejected' if accepted == 0 else item['state'] if accepted == 1 else 'acceptance_unknown'
                db.execute('''INSERT INTO channel_calls
                    (id,organization_id,caller_id,operation_id,digest,task_id,account_id,credential_version,
                     quota_subject_id,model_id,provider_request_id,state,accepted,reservation_amount,
                     reservation_unknown,created_at,updated_at,proxy_json)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,'0',?,?,?,?)''',
                    (item['id'], organization, 'legacy-video-mcp', item['operation_id'], item['digest'],
                     item['operation_id'], account, item['credential_version'], account, item['model_id'],
                     item['provider_request_id'], state, accepted, int(accepted != 0), item['created_at'], timestamp, json.dumps(proxy)))
        db.execute("INSERT INTO channel_metadata VALUES ('accounts_cutover',?)", (timestamp,))
        db.execute("INSERT INTO channel_metadata VALUES ('legacy_source_digest',?)",
                   (fingerprint(db, store, LEGACY_TABLES),))
        db.execute("INSERT INTO channel_metadata VALUES ('legacy_target_digest',?)",
                   (fingerprint(db, store, CHANNEL_TABLES),))
