"""Read-only installation-wide occupancy proof for protected upgrades."""
from __future__ import annotations

import time


class ActivityUnavailable(Exception):
    pass


def activity(gateway, *, max_pending=16, budget_seconds=4):
    deadline = time.monotonic() + budget_seconds
    terminal = {'completed', 'failed', 'rejected'}
    known = terminal | {'queued', 'in_queue', 'in_progress',
                        'submission_unconfirmed', 'acceptance_unknown'}
    # Keep this connection open: data_version detects writes by other connections
    # throughout both reads and supplier requests, without blocking submissions.
    with gateway.store.connect() as db:
        before = db.execute('PRAGMA data_version').fetchone()[0]
        query = '''SELECT s.id,s.organization_id,s.credential_version,
                          s.provider_request_id,s.state,'legacy' AS source
                   FROM submissions s
                   UNION ALL
                   SELECT c.id,c.organization_id,c.credential_version,
                          c.provider_request_id,c.state,'channel' AS source
                   FROM channel_calls c
                   WHERE NOT EXISTS (SELECT 1 FROM submissions s WHERE s.id=c.id)'''
        total = db.execute('SELECT COUNT(*) FROM (' + query + ')').fetchone()[0]
        pending = db.execute('SELECT * FROM (' + query + ") WHERE state NOT IN ('completed','failed','rejected') LIMIT ?",
                             (max_pending + 1,)).fetchall()
        if len(pending) > max_pending:
            raise ActivityUnavailable()
        active = 0
        for row in pending:
            item = dict(row)
            if item['state'] not in known or time.monotonic() >= deadline:
                raise ActivityUnavailable()
            if not item['provider_request_id']:
                active += 1
                continue
            try:
                legacy_transport = item['source'] == 'legacy'
                if item['source'] == 'legacy':
                    key = gateway.store.decrypt(item['organization_id'], item['credential_version'])
                    try:
                        imported = gateway.calls.get(item['organization_id'], item['id'], 'legacy-video-mcp')
                    except KeyError:
                        pass
                    else:
                        item['proxy_json'] = imported['proxy_json']
                        legacy_transport = False
                else:
                    record = gateway.calls.get(item['organization_id'], item['id'])
                    key = gateway.calls.credential(record)
                    item = {**record, 'source': 'channel'}
                # _remote performs a GET using the original request identity;
                # unlike remote()/remote_call(), it does not change local state.
                observed = gateway._remote(item, key, False,
                    timeout=min(2, max(.01, deadline - time.monotonic())), legacy=legacy_transport)
                if observed['state'] not in {'completed', 'in_queue', 'in_progress'}:
                    raise ActivityUnavailable()
                active += observed['state'] != 'completed'
            except Exception:
                # Do not expose supplier responses, credential errors or IDs.
                raise ActivityUnavailable() from None
        if time.monotonic() >= deadline or db.execute('PRAGMA data_version').fetchone()[0] != before:
            raise ActivityUnavailable()
    return {'schema_version': 1, 'scope': 'installation',
            'submitted_count': total, 'active_count': active}
