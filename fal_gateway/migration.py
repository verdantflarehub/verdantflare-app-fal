"""Atomic cutover from organization credentials to durable channel accounts."""
import json
import uuid

from .store import now


def migrate_legacy(store, *, legacy_proxy_configured=False):
    """Run after Accounts/Calls schema initialization and before accepting requests."""
    with store.connect() as db:
        db.execute('BEGIN IMMEDIATE')
        if db.execute("SELECT 1 FROM channel_metadata WHERE name='accounts_cutover'").fetchone():
            return
        timestamp = now()
        organizations = db.execute('SELECT * FROM organizations ORDER BY organization_id').fetchall()
        if legacy_proxy_configured and db.execute('SELECT 1 FROM credentials LIMIT 1').fetchone():
            # Never stamp a proxied historical account as direct. The Network
            # reference must be resolved before this cutover can be enabled.
            raise ValueError('legacy_proxy_mapping_required')
        for org in organizations:
            organization = org['organization_id']
            credentials = db.execute('SELECT * FROM credentials WHERE organization_id=? ORDER BY version',
                                     (organization,)).fetchall()
            if not credentials:
                if db.execute('SELECT 1 FROM submissions WHERE organization_id=? LIMIT 1', (organization,)).fetchone():
                    raise ValueError('legacy_recovery_credential_missing')
                continue
            account = str(uuid.uuid4())
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
                (account, organization, 'fal.ai 原有账号', 'https://queue.fal.run', '{"mode":"direct"}',
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
                     reservation_unknown,created_at,updated_at)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,'0',?,?,?)''',
                    (item['id'], organization, 'legacy-video-mcp', item['operation_id'], item['digest'],
                     item['operation_id'], account, item['credential_version'], account, item['model_id'],
                     item['provider_request_id'], state, accepted, int(accepted != 0), item['created_at'], timestamp))
        db.execute("INSERT INTO channel_metadata VALUES ('accounts_cutover',?)", (timestamp,))
