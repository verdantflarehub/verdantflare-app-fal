"""Authenticated, fenced channel publication. No account or credential metadata."""
from __future__ import annotations

import base64
from dataclasses import dataclass, field
from datetime import UTC, datetime
import json
import re
import uuid
from urllib.parse import urlsplit

import httpx

IDENTITY = re.compile(r'[a-zA-Z0-9][a-zA-Z0-9._-]{0,127}\Z')
STATES = frozenset({'enabled', 'draining', 'disabled', 'degraded', 'unavailable'})


class DiscoveryError(RuntimeError):
    """Only a stable code crosses the registry boundary."""


class Superseded(DiscoveryError):
    pass


def encode(value):
    return base64.b64encode(value.encode()).decode()


def document(value):
    return encode(json.dumps(value, sort_keys=True, separators=(',', ':')))


def prefix_end(value):
    return value[:-1] + chr(ord(value[-1]) + 1)


@dataclass(frozen=True)
class Config:
    endpoint: str
    scope: str
    username: str = field(repr=False)
    password: str = field(repr=False)
    channel: str = 'fal'
    execution_url: str = 'http://video-fal-gateway.verdantflare-video.svc.cluster.local:8001'

    def __post_init__(self):
        if (not IDENTITY.fullmatch(self.scope) or self.channel != 'fal'
                or not self.username or not self.password):
            raise ValueError('invalid_discovery_configuration')
        parsed = urlsplit(self.endpoint)
        try:
            valid = (parsed.scheme in ('http', 'https') and parsed.hostname
                     and parsed.hostname.endswith('.svc.cluster.local')
                     and parsed.port in (None, 2379) and not parsed.username and not parsed.password
                     and parsed.path in ('', '/') and not parsed.query and not parsed.fragment)
        except ValueError:
            valid = False
        if not valid or self.execution_url != 'http://video-fal-gateway.verdantflare-video.svc.cluster.local:8001':
            raise ValueError('invalid_discovery_configuration')

    @property
    def prefix(self):
        return f'/verdantflare/channels/v1/{self.scope}/{self.channel}/'


class Etcd:
    def __init__(self, config, *, transport=None):
        self.config = config
        self.client = httpx.Client(base_url=config.endpoint.rstrip('/'), timeout=10,
                                  trust_env=False, follow_redirects=False, transport=transport)
        self._token = None

    def _send(self, path, body, token=None):
        try:
            response = self.client.post('/v3/' + path, json=body,
                headers={'Authorization': token} if token else {})
            value = response.json()
            if not isinstance(value, dict):
                raise ValueError()
        except (httpx.HTTPError, ValueError):
            raise DiscoveryError('registry_unavailable') from None
        return response.status_code, value

    def request(self, path, body):
        for attempt in range(2):
            if not self._token:
                status, value = self._send('auth/authenticate', {
                    'name': self.config.username, 'password': self.config.password})
                if status != 200 or not isinstance(value.get('token'), str) or not value['token']:
                    raise DiscoveryError('registry_authentication_failed')
                self._token = value['token']
            status, value = self._send(path, body, self._token)
            if status == 401 or value.get('code') == 16:
                self._token = None
                if attempt == 0:
                    continue
            if status != 200 or 'error' in value or value.get('code'):
                raise DiscoveryError('registry_request_rejected')
            return value
        raise DiscoveryError('registry_authentication_failed')

    def close(self):
        self._token = None
        self.client.close()


class Publisher:
    """One instance per execution-owner lock; SQLite generations survive restart.

    Publication is atomic and fenced by status mod_revision. A conflicting writer
    permanently fences this object, even if its process keeps running. Discovery
    failures never submit, retry, or alter a provider task.
    """
    def __init__(self, config, store, version, capabilities, *, etcd=None):
        self.config, self.store, self.version = config, store, version
        # Only a closed capability summary is accepted, never arbitrary metadata.
        self.capabilities = []
        for capability in capabilities:
            if (set(capability) != {'capability_id', 'adapter_version', 'execution_state'}
                    or not all(isinstance(v, str) and IDENTITY.fullmatch(v) for v in capability.values())
                    or capability['execution_state'] not in {'enabled', 'disabled', 'catalog_only', 'adapter_connected_unverified'}):
                raise ValueError('invalid_capability_summary')
            self.capabilities.append(dict(capability))
        if not isinstance(version, str) or not IDENTITY.fullmatch(version):
            raise ValueError('invalid_discovery_version')
        self.etcd = etcd or Etcd(config)
        self.instance = str(uuid.uuid4())
        self.lease = None
        self.mod_revision = None
        self.fenced = False
        self.closed = False
        self.status_key = config.prefix + 'status'
        self.instance_key = config.prefix + 'instances/' + self.instance
        with store.connect() as db:
            db.execute('CREATE TABLE IF NOT EXISTS channel_discovery ('
                       'scope TEXT PRIMARY KEY, generation INTEGER NOT NULL, '
                       'state TEXT NOT NULL, reason TEXT NOT NULL, revision INTEGER NOT NULL, '
                       'resume_state TEXT, resume_reason TEXT)')
            db.execute('BEGIN IMMEDIATE')
            db.execute("INSERT OR IGNORE INTO channel_discovery (scope,generation,state,reason,revision) "
                       "VALUES (?,0,'enabled','ready',1)", (config.scope,))
            db.execute('UPDATE channel_discovery SET generation=generation+1 WHERE scope=?', (config.scope,))
            self.generation = db.execute('SELECT generation FROM channel_discovery WHERE scope=?', (config.scope,)).fetchone()[0]

    def state(self):
        with self.store.connect() as db:
            row = db.execute('SELECT state,reason,revision FROM channel_discovery WHERE scope=?', (self.config.scope,)).fetchone()
        return {'state': row[0], 'reason_code': row[1], 'revision': row[2],
                'observed_at': datetime.now(UTC).isoformat()}

    def set_state(self, state, reason):
        if state not in STATES or not isinstance(reason, str) or not IDENTITY.fullmatch(reason):
            raise ValueError('invalid_channel_state')
        with self.store.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            row = db.execute('SELECT generation FROM channel_discovery WHERE scope=?', (self.config.scope,)).fetchone()
            if row[0] != self.generation:
                self.fenced = True
                raise Superseded('registry_instance_superseded')
            db.execute('UPDATE channel_discovery SET state=?,reason=?,revision=revision+1, '
                       'resume_state=NULL,resume_reason=NULL WHERE scope=?',
                       (state, reason, self.config.scope))

    def drain(self):
        with self.store.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            row = db.execute('SELECT * FROM channel_discovery WHERE scope=?', (self.config.scope,)).fetchone()
            if row['generation'] != self.generation:
                self.fenced = True
                raise Superseded('registry_instance_superseded')
            if row['state'] == 'draining':
                return
            db.execute("UPDATE channel_discovery SET resume_state=state,resume_reason=reason, "
                       "state='draining',reason='process_stopping',revision=revision+1 WHERE scope=?",
                       (self.config.scope,))

    def resume(self):
        with self.store.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            row = db.execute('SELECT * FROM channel_discovery WHERE scope=?', (self.config.scope,)).fetchone()
            if row['generation'] != self.generation:
                self.fenced = True
                raise Superseded('registry_instance_superseded')
            if row['state'] == 'draining' and row['reason'] == 'process_stopping' and row['resume_state']:
                db.execute('UPDATE channel_discovery SET state=resume_state,reason=resume_reason, '
                           'resume_state=NULL,resume_reason=NULL,revision=revision+1 WHERE scope=?',
                           (self.config.scope,))

    def _current_generation(self):
        with self.store.connect() as db:
            return db.execute('SELECT generation FROM channel_discovery WHERE scope=?', (self.config.scope,)).fetchone()[0]

    def _snapshot(self):
        result = self.etcd.request('kv/range', {'key': encode(self.config.prefix),
            'range_end': encode(prefix_end(self.config.prefix))})
        if result.get('more') or len(result.get('kvs', [])) > 100:
            raise DiscoveryError('registry_invalid_snapshot')
        status_revision = 0
        for kv in result.get('kvs', []):
            try:
                key = base64.b64decode(kv['key'], validate=True).decode()
                if key == self.status_key:
                    status_revision = int(kv['mod_revision'])
                elif key.startswith(self.config.prefix + 'instances/'):
                    entry = json.loads(base64.b64decode(kv['value'], validate=True))
                    generation = entry['generation']
                    if type(generation) is not int or generation < 1:
                        raise ValueError()
                    if generation >= self.generation and key != self.instance_key:
                        self.fenced = True
                        raise Superseded('registry_instance_superseded')
                else:
                    raise ValueError()
            except (ValueError, KeyError, TypeError):
                raise DiscoveryError('registry_invalid_snapshot') from None
        return status_revision

    def _revoke(self):
        lease, self.lease = self.lease, None
        self.mod_revision = None
        if lease:
            try:
                self.etcd.request('lease/revoke', {'ID': lease})
            except DiscoveryError:
                # Failed cleanup expires by TTL; never revoke another instance lease.
                pass

    def publish(self):
        if self.closed or self.fenced or self._current_generation() != self.generation:
            self.fenced = True
            raise Superseded('registry_instance_superseded')
        try:
            if self.lease is None:
                self.mod_revision = self._snapshot()
                result = self.etcd.request('lease/grant', {'TTL': 300})
                lease = str(result.get('ID', ''))
                if not lease.isdigit() or int(lease) < 1 or int(result.get('TTL', 0)) < 1:
                    raise DiscoveryError('registry_invalid_lease')
                self.lease = lease
            else:
                result = self.etcd.request('lease/keepalive', {'ID': self.lease}).get('result', {})
                if str(result.get('ID')) != self.lease or int(result.get('TTL', 0)) <= 0:
                    raise DiscoveryError('registry_lease_expired')
            entry = {'schema_version': 1, 'version': self.version,
                'execution_url': self.config.execution_url, 'service_identity': self.config.channel,
                'generation': self.generation, 'adapter_version': '1', 'capabilities': self.capabilities}
            transaction = {'compare': [{'key': encode(self.status_key), 'target': 'MOD',
                'result': 'EQUAL', 'mod_revision': str(self.mod_revision)}], 'success': [
                {'request_put': {'key': encode(self.instance_key), 'value': document(entry), 'lease': self.lease}},
                {'request_put': {'key': encode(self.status_key), 'value': document(self.state()), 'lease': self.lease}}],
                'failure': []}
            result = self.etcd.request('kv/txn', transaction)
            if result.get('succeeded') is not True:
                self.fenced = True
                raise Superseded('registry_instance_superseded')
            revision = int(result['header']['revision'])
            if revision <= self.mod_revision:
                raise DiscoveryError('registry_invalid_revision')
            self.mod_revision = revision
        except (DiscoveryError, ValueError, TypeError, KeyError):
            self._revoke()
            raise DiscoveryError('registry_publication_failed') from None

    def close(self):
        if not self.closed:
            self._revoke()
            self.etcd.close()
            self.closed = True
