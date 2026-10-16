"""Consume authorized Network references; never store proxy credentials."""
import ipaddress
import re
import uuid
from urllib.parse import quote, urlsplit

import httpx


class ProxyUnavailable(ValueError):
    def __init__(self):
        super().__init__('proxy_unavailable')


def proxy_reference(value):
    if value == {'mode': 'direct'}:
        return dict(value)
    if not isinstance(value, dict) or set(value) != {'mode', 'proxy_id', 'revision'} or value['mode'] != 'proxy':
        raise ProxyUnavailable()
    try:
        identifier = uuid.UUID(value['proxy_id'])
        if not identifier.int or str(identifier) != value['proxy_id']:
            raise ValueError()
    except (ValueError, TypeError, AttributeError):
        raise ProxyUnavailable() from None
    if type(value['revision']) is not int or not 1 <= value['revision'] <= 2**53 - 1:
        raise ProxyUnavailable()
    return dict(value)


class NetworkProxies:
    def __init__(self, origin, token, *, client=None):
        parsed = urlsplit(origin)
        if (parsed.scheme not in {'http', 'https'} or not parsed.hostname or parsed.username
                or parsed.password or parsed.path not in {'', '/'} or parsed.query or parsed.fragment
                or len(token) < 32 or any(c.isspace() for c in token)):
            raise ValueError('invalid_network_configuration')
        self.origin, self.token = origin.rstrip('/'), token
        self.client = client or httpx.Client(trust_env=False, follow_redirects=False, timeout=5)

    def close(self):
        self.client.close()

    def request(self, organization, action, payload):
        try:
            parsed = uuid.UUID(organization)
            if not parsed.int or str(parsed) != organization:
                raise ValueError()
            with self.client.stream('POST', self.origin + '/internal/channel-proxies/v1/' + action,
                    headers={'Authorization': 'Bearer ' + self.token, 'X-Organization-Id': organization},
                    json={'schema_version': 1, **payload}) as response:
                if response.status_code != 200:
                    raise ProxyUnavailable()
                data = bytearray()
                for chunk in response.iter_bytes():
                    data.extend(chunk)
                    if len(data) > 256 * 1024:
                        raise ProxyUnavailable()
                import json
                def unique(pairs):
                    result = {}
                    for key, value in pairs:
                        if key in result:
                            raise ValueError()
                        result[key] = value
                    return result
                result = json.loads(data, object_pairs_hook=unique)
                if not isinstance(result, dict) or type(result.get('schema_version')) is not int or result['schema_version'] != 1:
                    raise ProxyUnavailable()
                return result
        except (httpx.HTTPError, ValueError, TypeError, AttributeError):
            raise ProxyUnavailable() from None

    def list(self, organization):
        result = self.request(organization, 'list', {})
        if set(result) != {'schema_version', 'items'} or not isinstance(result['items'], list) or len(result['items']) > 128:
            raise ProxyUnavailable()
        seen = set()
        for item in result['items']:
            if not isinstance(item, dict) or set(item) != {'proxy_id', 'name', 'protocol', 'status', 'revision', 'expires_at'}:
                raise ProxyUnavailable()
            proxy_reference({'mode': 'proxy', 'proxy_id': item['proxy_id'], 'revision': item['revision']})
            if item['proxy_id'] in seen or not isinstance(item['name'], str) or len(item['name']) > 200 or item['status'] not in {'active', 'pending', 'off'} or item['protocol'] not in {'http', 'https', 'socks5', 'socks5h'}:
                raise ProxyUnavailable()
            seen.add(item['proxy_id'])
        return result

    def resolve_url(self, organization, reference):
        reference = proxy_reference(reference)
        if reference['mode'] != 'proxy':
            raise ProxyUnavailable()
        value = self.request(organization, 'resolve', {k: reference[k] for k in ('proxy_id', 'revision')})
        if (set(value) != {'schema_version', 'proxy_id', 'revision', 'protocol', 'host', 'port', 'username', 'password'}
                or value['proxy_id'] != reference['proxy_id'] or type(value['revision']) is not int
                or value['revision'] != reference['revision'] or value['protocol'] not in {'http', 'https', 'socks5', 'socks5h'}):
            raise ProxyUnavailable()
        host = value['host']
        if not isinstance(host, str) or not host or len(host) > 253:
            raise ProxyUnavailable()
        if ':' in host:
            try:
                host = '[' + str(ipaddress.IPv6Address(host)) + ']'
            except ValueError:
                raise ProxyUnavailable() from None
        elif not re.fullmatch(r'[A-Za-z0-9](?:[A-Za-z0-9.-]*[A-Za-z0-9])?', host):
            raise ProxyUnavailable()
        if type(value['port']) is not int or not 1 <= value['port'] <= 65535:
            raise ProxyUnavailable()
        for name in ('username', 'password'):
            if not isinstance(value[name], str) or len(value[name]) > 255 or any(c in value[name] for c in '\0\r\n'):
                raise ProxyUnavailable()
        if value['password'] and not value['username']:
            raise ProxyUnavailable()
        auth = quote(value['username'], safe='') + ':' + quote(value['password'], safe='') + '@' if value['username'] else ''
        return value['protocol'] + '://' + auth + host + ':' + str(value['port'])
