"""Two authenticated internal listeners; no public route or browser credential storage."""
from __future__ import annotations

import asyncio
import hmac
import json
import os
from pathlib import Path
import re
import uuid
from contextlib import contextmanager
from urllib.parse import urlsplit

import httpx
from starlette.applications import Starlette
from starlette.concurrency import run_in_threadpool
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route
import uvicorn

from . import __version__
from .dashboard import DashboardAssets
from .store import Conflict, Store
from .accounts import Accounts
from .calls import Calls
from .ownership import execution_owner
from .discovery_lifecycle import configured_publisher, PublicationLifetime, serve_owned_servers
from .migration import backup_legacy, migrate_legacy, resolve_legacy_proxies
from .activity import activity, ActivityUnavailable
from .proxy import NetworkProxies, ProxyUnavailable, proxy_reference
from .capabilities import load_metadata
from .billing import BillingError, estimate, fetch_bill
from .errors import provider_failure, failure

MODEL = "minimax/h3/reference-to-video"
SD2_MODEL = "bytedance/seedance-2.0/reference-to-video"
CALL_MODELS = {MODEL, SD2_MODEL}
REQUEST_PATHS = {MODEL: "minimax/h3", SD2_MODEL: "bytedance/seedance-2.0"}
QUEUE = "https://queue.fal.run"
CHECK = "https://api.fal.ai/v1/models/requests/by-endpoint"
IDENTITY = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}")
REQUEST_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,255}")
PAYLOAD_FIELDS = {"duration", "resolution", "enable_safety_checker", "sync_mode",
                  "prompt_expansion_mode", "aspect_ratio", "prompt",
                  "reference_image_urls", "reference_video_urls", "reference_audio_urls"}
ASPECT_RATIOS = {"adaptive", "21:9", "16:9", "4:3", "1:1", "3:4", "9:16"}


class APIError(Exception):
    def __init__(self, code: str, status: int = 400, diagnostic=None):
        self.code, self.status, self.diagnostic = code, status, diagnostic


def respond(value: dict, status: int = 200):
    if status >= 400:
        code = str(value.get('error', '')).upper()
        value = {**value, 'code': code if re.fullmatch(r'[A-Z][A-Z0-9_]{0,63}', code) else 'REQUEST_FAILED'}
    return JSONResponse(value, status_code=status, headers={"Cache-Control": "no-store"})


async def body(request: Request, limit: int = 32768) -> dict:
    chunks = bytearray()
    async for chunk in request.stream():
        chunks.extend(chunk)
        if len(chunks) > limit:
            raise APIError("request_too_large", 413)
    try:
        def unique(pairs):
            value = {}
            for key, item in pairs:
                if key in value:
                    raise ValueError("duplicate_key")
                value[key] = item
            return value
        result = json.loads(chunks, object_pairs_hook=unique)
    except (ValueError, UnicodeDecodeError):
        raise APIError("invalid_request") from None
    if not isinstance(result, dict):
        raise APIError("invalid_request")
    return result


def number(value, minimum: int, maximum: int) -> int:
    if type(value) is not int or not minimum <= value <= maximum:
        raise APIError("invalid_request")
    return value


def identity(value: str) -> str:
    if not isinstance(value, str) or not IDENTITY.fullmatch(value):
        raise APIError("invalid_identity", 403)
    return value


async def filters(request, allowed):
    query = dict(await body(request) if request.method == 'POST' else request.query_params)
    if set(query) - set(allowed):
        raise APIError('invalid_filter')
    for key, value in query.items():
        if key in {'offset', 'limit'}:
            if request.method == 'GET' and isinstance(value, str) and value.isascii() and value.isdigit():
                value = int(value)
            query[key] = number(value, 0 if key == 'offset' else 1, 2**31 - 1 if key == 'offset' else 100)
        elif not isinstance(value, str) or len(value) > 200:
            raise APIError('invalid_filter')
    return query


def authenticate(request: Request, token: str, *, manage: bool) -> str:
    headers = request.headers.getlist("authorization")
    if len(token) < 32 or len(headers) != 1 or not hmac.compare_digest(headers[0], "Bearer " + token):
        raise APIError("unauthorized", 401)
    if manage:
        identity(request.headers.get("x-user-id", ""))
    return identity(request.headers.get("x-organization-id", ""))


class Gateway:
    def __init__(self, store: Store, client: httpx.Client | None = None, *, network=None):
        self.store = store
        self.accepting = True
        self.calls = Calls(store)
        # Explicit direct accounts never inherit deployment or host proxies.
        self.client = client or httpx.Client(follow_redirects=False, timeout=20, trust_env=False)
        self._legacy_client = client
        self.network = network

    def validate_proxy(self, organization, value):
        value = proxy_reference(value)
        if value['mode'] == 'proxy':
            if self.network is None:
                raise ProxyUnavailable()
            self.network.resolve_url(organization, value)
        return value

    @contextmanager
    def transport(self, organization, value, *, legacy=False):
        if legacy:
            yield self.legacy_client
            return
        value = proxy_reference(value)
        if value['mode'] == 'direct':
            yield self.client
            return
        if self.network is None:
            raise ProxyUnavailable()
        try:
            client = httpx.Client(proxy=self.network.resolve_url(organization, value),
                                 trust_env=False, follow_redirects=False, timeout=20)
        except (ValueError, ImportError):
            raise ProxyUnavailable() from None
        # Do not catch errors across yield: after a supplier request starts,
        # transport errors must remain acceptance-unknown, never local rejection.
        try:
            yield client
        finally:
            client.close()

    @property
    def legacy_client(self):
        # Only original submissions and original credential management use the
        # unchanged legacy deployment transport until their Network mapping exists.
        if self._legacy_client is None:
            self._legacy_client = httpx.Client(follow_redirects=False, timeout=20, trust_env=True)
        return self._legacy_client

    def close(self):
        self.client.close()
        if self._legacy_client is not None and self._legacy_client is not self.client:
            self._legacy_client.close()
        if self.network is not None:
            self.network.close()

    def check(self, key: str, *, legacy=False, organization='', proxy=None) -> str:
        try:
            with self.transport(organization, proxy or {'mode': 'direct'}, legacy=legacy) as client:
                response = client.get(CHECK, params={"endpoint_id": MODEL, "limit": 1},
                                      headers={"Authorization": "Key " + key}, timeout=15)
        except (httpx.HTTPError, ProxyUnavailable):
            return "unavailable"
        if response.status_code in {401, 403}:
            return "invalid"
        if response.status_code != 200:
            return "unavailable"
        try:
            data = response.json()
            return "active" if isinstance(data.get("items"), list) else "unavailable"
        except (ValueError, AttributeError):
            return "unavailable"

    @staticmethod
    def validate_payload(value, model=MODEL) -> dict:
        if model == SD2_MODEL:
            fields = {"prompt", "duration", "resolution", "aspect_ratio", "generate_audio",
                      "image_urls", "video_urls", "audio_urls"}
            if not isinstance(value, dict) or not value or set(value) - fields:
                raise APIError("invalid_payload")
            if not isinstance(value.get("prompt"), str) or not value["prompt"].strip():
                raise APIError("invalid_payload")
            number(value.get("duration"), 4, 15)
            if (value.get("resolution") not in {"480p", "720p"}
                    or value.get("aspect_ratio") not in {"16:9", "9:16", "1:1", "4:3", "3:4", "21:9"}
                    or value.get("generate_audio") is not True):
                raise APIError("unsupported_profile")
            for field, limit, prefix in (("image_urls", 9, "data:image/"),
                                         ("video_urls", 3, "data:video/"),
                                         ("audio_urls", 3, "data:audio/")):
                refs = value.get(field, [])
                if not isinstance(refs, list) or len(refs) > limit or any(
                        not isinstance(item, str) or not item.startswith(prefix) for item in refs):
                    raise APIError("invalid_payload")
            if not value.get("image_urls") and not value.get("video_urls"):
                raise APIError("reference_required")
            if sum(len(value.get(field, [])) for field in ("image_urls", "video_urls", "audio_urls")) > 12:
                raise APIError("invalid_payload")
            return value
        if model != MODEL:
            raise APIError("capability_not_supported")
        if not isinstance(value, dict) or not value or set(value) - PAYLOAD_FIELDS:
            raise APIError("invalid_payload")
        if not isinstance(value.get("prompt"), str) or not value["prompt"].strip():
            raise APIError("invalid_payload")
        if (value.get("resolution") != "480P" or value.get("enable_safety_checker") is not True
                or value.get("sync_mode") is not False or value.get("prompt_expansion_mode") != "disabled"
                or value.get("aspect_ratio") not in ASPECT_RATIOS):
            raise APIError("unsupported_profile")
        number(value.get("duration"), 5, 15)
        for field in ("reference_image_urls", "reference_video_urls", "reference_audio_urls"):
            refs = value.get(field, [])
            if not isinstance(refs, list) or len(refs) > {"reference_image_urls": 9,
                    "reference_video_urls": 3, "reference_audio_urls": 3}[field]:
                raise APIError("invalid_payload")
            prefix = {"reference_image_urls": "data:image/", "reference_video_urls": "data:video/",
                      "reference_audio_urls": "data:audio/"}[field]
            if any(not isinstance(item, str) or not item.startswith(prefix) for item in refs):
                raise APIError("invalid_payload")
        if not any(value.get(field) for field in ("reference_image_urls", "reference_video_urls", "reference_audio_urls")):
            raise APIError("reference_required")
        if sum(len(value.get(field, [])) for field in ("reference_image_urls", "reference_video_urls", "reference_audio_urls")) > 12:
            raise APIError("invalid_payload")
        return value

    def submit(self, organization_id: str, operation_id: str, payload: dict) -> dict:
        item, fresh = self.store.reserve_submission(organization_id, operation_id, MODEL, payload, str(uuid.uuid4()))
        if not fresh:
            return {"id": item["id"], "state": item["state"]}
        key = self.store.decrypt(organization_id, item["credential_version"])
        try:
            response = self.legacy_client.post(QUEUE + "/" + MODEL, json=payload,
                                        headers={"Authorization": "Key " + key}, timeout=60)
        except httpx.ConnectError:
            self.store.reject_submission(item["id"])
            return {"id": item["id"], "state": "rejected"}
        except httpx.HTTPError:
            return {"id": item["id"], "state": "submission_unconfirmed"}
        if response.status_code in {400, 401, 403, 404, 422, 429}:
            self.store.reject_submission(item["id"])
            return {"id": item["id"], "state": "rejected"}
        if not response.is_success:
            return {"id": item["id"], "state": "submission_unconfirmed"}
        try:
            remote = response.json()["request_id"]
            if not isinstance(remote, str) or not REQUEST_ID.fullmatch(remote):
                raise ValueError
        except (ValueError, KeyError, TypeError):
            return {"id": item["id"], "state": "submission_unconfirmed"}
        self.store.confirm_submission(item["id"], remote)
        return {"id": item["id"], "state": "queued"}

    def remote(self, organization_id: str, submission_id: str, result: bool = False) -> dict:
        item = self.store.submission(organization_id, submission_id)
        if not item["provider_request_id"]:
            return {"id": item["id"], "state": item["state"]}
        key = self.store.decrypt(organization_id, item["credential_version"])
        try:
            imported = self.calls.get(organization_id, submission_id, 'legacy-video-mcp')
        except KeyError:
            return self._remote(item, key, result, legacy=True)
        else:
            response = self._remote({**item, 'proxy_json': imported['proxy_json']}, key, result)
            self.calls.observed(submission_id, 'failed' if response.get('failed') else response['state'], response.get('error'))
        return response

    def submit_call(self, org, caller, operation, task, payload, account, ceiling, model=MODEL):
        item, fresh = self.calls.reserve(org, caller, operation, task, model, payload, account, ceiling)
        if not fresh:
            return self.calls.project(item)
        key = self.calls.credential(item)
        try:
            with self.transport(org, json.loads(item['proxy_json'])) as client:
                response = client.post(QUEUE + '/' + model, json=payload,
                                       headers={'Authorization': 'Key ' + key}, timeout=60)
        except ProxyUnavailable:
            # Resolution failed before any supplier request was made.
            self.calls.rejected(item['id'], failure('proxy_unavailable', accepted=False))
            raise APIError('proxy_unavailable', 503) from None
        except httpx.HTTPError:
            # Even a connection interruption is not evidence of upstream non-acceptance.
            return self.calls.project(item)
        if response.status_code in {400, 401, 402, 403, 404, 422, 429}:
            try:
                error_body = response.json()
            except ValueError:
                error_body = None
            self.calls.rejected(item['id'], provider_failure(error_body, response.status_code, accepted=False))
        elif response.is_success:
            try:
                remote = response.json()['request_id']
                if isinstance(remote, str) and REQUEST_ID.fullmatch(remote):
                    self.calls.accepted(item['id'], remote)
            except (ValueError, KeyError, TypeError):
                pass
        return self.calls.project(self.calls.get(org, item['id'], caller))

    def remote_call(self, org, caller, call_id, result=False):
        item = self.calls.get(org, call_id, caller)
        if not item['provider_request_id']:
            return self.calls.project(item)
        response = self._remote(item, self.calls.credential(item), result)
        self.calls.observed(call_id, 'failed' if response.get('failed') else response['state'], response.get('error'))
        projected = self.calls.project(self.calls.get(org, call_id, caller))
        if 'video_url' in response:
            projected['video_url'] = response['video_url']
        return projected

    def refresh_settlement(self, org, actor, call_id, revision):
        item = self.calls.get(org, call_id)
        if item['revision'] != revision:
            raise Conflict('revision_conflict')
        try:
            with self.transport(org, json.loads(item['proxy_json'])) as client:
                bill = fetch_bill(client, self.calls.credential(item), item)
        except ProxyUnavailable:
            raise APIError('proxy_unavailable', 503) from None
        return self.calls.settle_bill(item, actor, bill)

    def _remote(self, item, key, result, *, timeout=30, legacy=False):
        model = item.get("model_id", MODEL)
        if model not in REQUEST_PATHS:
            raise APIError("capability_not_supported")
        url = QUEUE + "/" + REQUEST_PATHS[model] + "/requests/" + item["provider_request_id"] + ("" if result else "/status")
        try:
            with self.transport(item['organization_id'], json.loads(item.get('proxy_json', '{"mode":"direct"}')), legacy=legacy) as client:
                response = client.get(url, headers={"Authorization": "Key " + key}, timeout=timeout)
        except ProxyUnavailable:
            raise APIError('proxy_unavailable', 503) from None
        except httpx.HTTPError:
            raise APIError("provider_unavailable", 503) from None
        if not response.is_success:
            try:
                error_body = response.json()
            except ValueError:
                error_body = None
            diagnostic = provider_failure(error_body, response.status_code, accepted=True)
            raise APIError(diagnostic['code'], 429 if response.status_code == 429 else 503, diagnostic)
        try:
            data = response.json()
            if not isinstance(data, dict):
                raise ValueError
            if data.get('error') or data.get('error_type'):
                return {'id': item['id'], 'state': 'failed', 'failed': True,
                        'error': provider_failure(data, accepted=True)}
            if result:
                video = data["video"]["url"]
                host = urlsplit(video).hostname if isinstance(video, str) else None
                if (not isinstance(video, str) or urlsplit(video).scheme != "https" or
                        not host or not (host == "fal.media" or host.endswith(".fal.media")) or
                        urlsplit(video).username or urlsplit(video).password):
                    raise ValueError
                return {"id": item["id"], "state": "completed", "video_url": video}
            state = data["status"]
            if state not in {"IN_QUEUE", "IN_PROGRESS", "COMPLETED"}:
                raise ValueError
            failed = bool(data.get("error") or data.get("error_type"))
            return {"id": item["id"], "state": state.lower(), "metrics": data.get("metrics") if isinstance(data.get("metrics"), dict) else None,
                    "failed": failed, "error": provider_failure(data, accepted=True) if failed else None}
        except (ValueError, KeyError, TypeError):
            raise APIError("provider_response_invalid", 503) from None


def create_apps(gateway: Gateway | None = None, management_token: str | None = None,
                execution_token: str | None = None):
    metadata_path = os.environ.get("CHANNEL_METADATA_FILE")
    channel_metadata = load_metadata(metadata_path) if metadata_path else None
    if gateway is None:
        origin, token = os.environ.get('FAL_NETWORK_URL', ''), os.environ.get('FAL_NETWORK_TOKEN', '')
        network = NetworkProxies(origin, token) if origin or token else None
        gateway = Gateway(Store(os.environ.get("FAL_STATE_DB", "/data/fal/state.sqlite3"),
                                os.environ["FAL_ENCRYPTION_KEY"]), network=network)
        try:
            backup_legacy(gateway.store)
            legacy_url = next((os.environ[name] for name in ('https_proxy', 'HTTPS_PROXY', 'all_proxy', 'ALL_PROXY') if os.environ.get(name)), '')
            reference = json.loads(os.environ['FAL_LEGACY_PROXY_REFERENCE']) if os.environ.get('FAL_LEGACY_PROXY_REFERENCE') else None
            mappings = resolve_legacy_proxies(gateway.store, network, legacy_url, reference)
            migrate_legacy(gateway.store, legacy_proxy_configured=bool(legacy_url), legacy_proxies=mappings)
        except Exception:
            gateway.close()
            raise
    management_token = management_token if management_token is not None else os.environ.get("FAL_MANAGEMENT_TOKEN", "")
    execution_token = execution_token if execution_token is not None else os.environ.get("FAL_EXECUTION_TOKEN", "")
    dashboard_path = Path(os.environ.get("FAL_DASHBOARD_DIR", "/app/dashboard"))
    assets = DashboardAssets(dashboard_path) if dashboard_path.exists() else None
    accounts = Accounts(gateway.store)
    refreshes = {}
    log_refreshes = {}
    status_slots = asyncio.Semaphore(4)

    def observe_call(item):
        response = gateway._remote(item, gateway.calls.credential(item), False, timeout=2)
        gateway.calls.observed(item['id'], 'failed' if response.get('failed') else response['state'], response.get('error'))

    async def refresh_calls(items):
        async def observe(item):
            if item['state'] in {'completed', 'failed', 'rejected'} or not item['provider_request_id']:
                return 'unchanged'
            async with status_slots:
                try:
                    await run_in_threadpool(observe_call, item)
                    return 'refreshed'
                except (APIError, ValueError, KeyError):
                    return 'unavailable'
        results = await asyncio.gather(*(observe(item) for item in items))
        return {'schema_version': 1, **{name: results.count(name) for name in ('refreshed', 'unchanged', 'unavailable')}}

    def finish_log_refresh(key, task):
        if log_refreshes.get(key) is task:
            log_refreshes.pop(key)
        if not task.cancelled():
            task.exception()

    def finish_refresh(identity_key, finished):
        if refreshes.get(identity_key) is finished:
            refreshes.pop(identity_key)
        # A disconnected requester must not leave an unobserved task exception.
        if not finished.cancelled():
            finished.exception()

    async def refresh_account(org, actor, account_id):
        revision, key, proxy = accounts.connection(org, account_id)
        state = await run_in_threadpool(gateway.check, key, organization=org, proxy=proxy)
        account = accounts.checked(org, actor, account_id, revision, state)
        return {"schema_version": 1, "account": account,
                "authorization_state": state, "balance_status": "manual_only"}

    async def manage(request: Request):
        try:
            path = request.url.path
            if path.startswith("/internal/"):
                headers = request.headers.getlist("authorization")
                if len(management_token) < 32 or len(headers) != 1 or not hmac.compare_digest(headers[0], "Bearer " + management_token):
                    raise APIError("unauthorized", 401)
                if assets is None:
                    raise APIError("dashboard_unavailable", 503)
                if path == "/internal/dashboard-declaration":
                    data, mime = assets.declaration, "application/json"
                elif path == "/internal/dashboard-index":
                    data, mime = assets.index, "application/json"
                else:
                    try:
                        data, mime = assets.asset(request.path_params["asset"])
                    except (KeyError, ValueError, OSError):
                        raise APIError("dashboard_asset_unavailable", 404) from None
                return Response(data, headers={"Content-Type": mime, "Cache-Control": "no-store",
                                               "X-Content-Type-Options": "nosniff"})
            org = authenticate(request, management_token, manage=True)
            if path.endswith('/settlement/refresh') and 'call_id' in request.path_params:
                data = await body(request)
                if set(data) != {'expected_revision'}:
                    raise APIError('invalid_settlement_request')
                revision = number(data['expected_revision'], 1, 2**31 - 1)
                return respond(await run_in_threadpool(gateway.refresh_settlement, org,
                    request.headers['x-user-id'], str(request.path_params['call_id']), revision))
            if path == '/v1/calls/refresh':
                data = await body(request)
                ids = data.get('call_ids')
                if set(data) != {'call_ids'} or not isinstance(ids, list) or not 1 <= len(ids) <= 16:
                    raise APIError('invalid_refresh_request')
                if any(not isinstance(value, str) or str(uuid.UUID(value)) != value for value in ids) or len(set(ids)) != len(ids):
                    raise APIError('invalid_refresh_request')
                items = [gateway.calls.get(org, identity) for identity in ids]
                identity_key = (org, tuple(sorted(ids)))
                task = log_refreshes.get(identity_key)
                if task is None:
                    task = asyncio.create_task(refresh_calls(items))
                    log_refreshes[identity_key] = task
                    task.add_done_callback(lambda finished, key=identity_key: finish_log_refresh(key, finished))
                return respond(await asyncio.shield(task))
            if path == '/v1/proxies':
                if gateway.network is None:
                    return respond({'schema_version': 1, 'items': []})
                try:
                    return respond(await run_in_threadpool(gateway.network.list, org))
                except ProxyUnavailable:
                    raise APIError('proxy_unavailable', 503) from None
            if path == '/v1/activity':
                try:
                    return respond(await run_in_threadpool(activity, gateway))
                except ActivityUnavailable:
                    return respond({'error': 'activity_unavailable'}, 503)
            if path in {'/v1/calls', '/v1/calls/search'}:
                query = await filters(request, {'account_id', 'state', 'search', 'offset', 'limit',
                                                'media', 'created_from', 'created_before'})
                return respond(gateway.calls.list(org, account=query.get('account_id'),
                    state=query.get('state'), search=query.get('search', ''),
                    offset=int(query.get('offset', 0)), limit=int(query.get('limit', 50)),
                    media=query.get('media'), created_from=query.get('created_from'),
                    created_before=query.get('created_before')))
            if path in {"/v1/accounts", '/v1/accounts/search'}:
                if request.method == "GET" or path.endswith('/search'):
                    query = await filters(request, {'search', 'status', 'offset', 'limit'})
                    return respond(accounts.list(org, search=query.get('search', ''),
                        status=query.get('status', ''), offset=int(query.get('offset', 0)),
                        limit=int(query.get('limit', 50))))
                data = await body(request)
                operation = data.pop('operation_id', request.headers.get('idempotency-key', ''))
                if request.headers.get('idempotency-key', operation) != operation:
                    raise APIError('idempotency_key_mismatch')
                proxy_reference(data.get('proxy'))
                try:
                    created = await run_in_threadpool(accounts.create, org, request.headers['x-user-id'],
                        operation, data, validate_proxy=lambda value: gateway.validate_proxy(org, value))
                except ProxyUnavailable:
                    raise APIError('proxy_unavailable', 503) from None
                return respond(created, 201)
            if "account_id" in request.path_params:
                account_id = str(request.path_params["account_id"])
                if path.endswith("/refresh"):
                    identity_key = (org, account_id)
                    task = refreshes.get(identity_key)
                    if task is None:
                        task = asyncio.create_task(refresh_account(org, request.headers["x-user-id"], account_id))
                        refreshes[identity_key] = task
                        task.add_done_callback(lambda finished, identity_key=identity_key:
                            finish_refresh(identity_key, finished))
                    return respond(await asyncio.shield(task))
                if path.endswith("/credential"):
                    data = await body(request)
                    key = data.get("api_key")
                    if not isinstance(key, str) or not 16 <= len(key) <= 4096 or any(c.isspace() for c in key):
                        raise APIError("invalid_key")
                    revision = number(data.get("expected_revision"), 1, 2**31 - 1)
                    current_revision, _, proxy = accounts.connection(org, account_id)
                    if current_revision != revision:
                        raise Conflict("revision_conflict")
                    state = await run_in_threadpool(gateway.check, key, organization=org, proxy=proxy)
                    result = accounts.checked(org, request.headers["x-user-id"], account_id, revision, state, key)
                    return respond({"schema_version": 1, "account": result, "authorization_state": state},
                                   200 if state == "active" else 422 if state == "invalid" else 503)
                if request.method == "GET":
                    return respond(accounts.get(org, account_id))
                data = await body(request)
                revision = data.pop("expected_revision", None)
                if 'proxy' in data:
                    proxy_reference(data['proxy'])
                    try:
                        await run_in_threadpool(gateway.validate_proxy, org, data['proxy'])
                    except ProxyUnavailable:
                        raise APIError('proxy_unavailable', 503) from None
                return respond(accounts.patch(org, request.headers["x-user-id"], account_id, revision,
                    data, archive=request.method == "DELETE"))
            if path in {"/v1/overview", "/v1/authorization", "/v1/limits"} and request.method == "GET":
                return respond(gateway.store.overview(org))
            if path == "/v1/usage" and request.method == "GET":
                return respond(gateway.store.usage(org))
            if path == "/v1/authorization" and request.method == "PUT":
                data = await body(request)
                key = data.get("api_key")
                if not isinstance(key, str) or not 16 <= len(key) <= 4096 or any(c.isspace() for c in key):
                    raise APIError("invalid_key")
                revision = number(data.get("expected_revision"), 0, 2**31 - 1)
                version = gateway.store.candidate(org, key, revision)
                state = await run_in_threadpool(gateway.check, key, legacy=True)
                if state == "active":
                    return respond(gateway.store.activate(org, version, revision))
                gateway.store.mark_candidate(org, version, state)
                return respond({"status": state, "active": gateway.store.overview(org)},
                               422 if state == "invalid" else 503)
            if path == "/v1/authorization" and request.method == "DELETE":
                revision = number((await body(request)).get("expected_revision"), 0, 2**31 - 1)
                return respond(gateway.store.revoke(org, revision))
            if path == "/v1/authorization/check" and request.method == "POST":
                current = gateway.store.overview(org)
                if not current["version"]:
                    raise APIError("authorization_required", 409)
                key = gateway.store.decrypt(org, current["version"])
                state = await run_in_threadpool(gateway.check, key, legacy=True)
                return respond(gateway.store.recheck(org, state))
            if path == "/v1/limits" and request.method == "PUT":
                data = await body(request)
                if type(data.get("enabled")) is not bool:
                    raise APIError("invalid_request")
                return respond(gateway.store.set_limits(org, data["enabled"],
                    number(data.get("daily_limit"), 0, 1000),
                    number(data.get("expected_revision"), 0, 2**31 - 1)))
            raise APIError("not_found", 404)
        except BillingError as error:
            return respond({'error': error.code}, error.status)
        except APIError as error:
            return respond({"error": error.code, **({'diagnostic': error.diagnostic} if error.diagnostic else {})}, error.status)
        except Conflict as error:
            return respond({"error": str(error)}, 409)
        except KeyError:
            return respond({"error": "call_not_found" if '/calls/' in request.url.path else "account_not_found"}, 404)
        except ValueError:
            return respond({"error": "invalid_account_request"}, 400)

    async def execute(request: Request):
        try:
            if request.url.path == '/v1/capabilities' and request.method == 'GET':
                auth = request.headers.getlist('authorization')
                if (len(execution_token) < 32 or len(auth) != 1
                        or not hmac.compare_digest(auth[0], 'Bearer ' + execution_token)):
                    raise APIError('unauthorized', 401)
                if request.headers.get('x-caller-id') != 'video-mcp':
                    raise APIError('forbidden', 403)
                if channel_metadata is None:
                    raise APIError('capabilities_unavailable', 503)
                return respond(channel_metadata)
            org = authenticate(request, execution_token, manage=False)
            if request.url.path == '/v1/cost':
                if request.headers.get('x-caller-id') != 'video-mcp':
                    raise APIError('forbidden', 403)
                data = await body(request, 96 * 1024 * 1024)
                if (set(data) != {'model_id', 'payload'} or not isinstance(data.get('model_id'), str)
                        or data['model_id'] not in CALL_MODELS):
                    raise APIError('invalid_request')
                payload = gateway.validate_payload(data['payload'], data['model_id'])
                return respond(estimate(data['model_id'], payload))
            if request.method == 'POST' and not gateway.accepting:
                return respond({'error': 'channel_unavailable', 'schema_version': 1,
                                'accepted': False, 'state': 'rejected'}, 503)
            if request.url.path.startswith('/v1/calls'):
                caller = identity(request.headers.get('x-caller-id', ''))
                if request.method == 'POST':
                    data = await body(request, 96 * 1024 * 1024)
                    model = data.get('model_id')
                    if not isinstance(model, str) or model not in CALL_MODELS or data.get('capability') != 'video':
                        raise APIError('capability_not_supported')
                    operation = identity(data.get('idempotency_key', ''))
                    task = identity(data.get('task_id', ''))
                    account = data.get('account_id')
                    if account is not None:
                        try:
                            account = str(uuid.UUID(account))
                        except (ValueError, TypeError, AttributeError):
                            raise APIError('invalid_account') from None
                    payload = gateway.validate_payload(data.get('payload'), model)
                    return respond(await run_in_threadpool(gateway.submit_call, org, caller, operation,
                        task, payload, account, data.get('budget_limit'), model))
                if 'operation_id' in request.path_params:
                    return respond(gateway.calls.by_operation(org, caller, request.path_params['operation_id']))
                return respond(await run_in_threadpool(gateway.remote_call, org, caller,
                    str(request.path_params['call_id']), request.url.path.endswith('/result')))
            if request.url.path == "/v1/submissions" and request.method == "POST":
                data = await body(request, 96 * 1024 * 1024)
                operation_id = identity(data.get("operation_id", ""))
                if data.get("model_id") != MODEL:
                    raise APIError("unsupported_model")
                payload = gateway.validate_payload(data.get("payload"))
                return respond(await run_in_threadpool(gateway.submit, org, operation_id, payload))
            if "submission_id" in request.path_params:
                return respond(await run_in_threadpool(gateway.remote, org,
                    str(request.path_params["submission_id"]), request.url.path.endswith("/result")))
            raise APIError("not_found", 404)
        except BillingError as error:
            return respond({'error': error.code}, error.status)
        except APIError as error:
            return respond({"error": error.code, **({'diagnostic': error.diagnostic} if error.diagnostic else {})}, error.status)
        except Conflict as error:
            return respond({"error": str(error)}, 409)
        except KeyError:
            return respond({"error": "call_not_found" if request.url.path.startswith('/v1/calls') else "submission_not_found"}, 404)
        except ValueError:
            return respond({'error': 'invalid_request'}, 400)

    async def health(request):
        return respond({"service": "video-fal-gateway", "version": __version__, "status": "ok"})

    management = Starlette(routes=[Route("/health", health), Route('/v1/activity', manage), Route('/v1/proxies', manage),
        Route("/internal/dashboard-declaration", manage), Route("/internal/dashboard-index", manage),
        Route("/internal/dashboard/{asset:path}", manage),
        Route('/v1/calls', manage),
        Route('/v1/calls/search', manage, methods=['POST']),
        Route('/v1/calls/refresh', manage, methods=['POST']),
        Route('/v1/calls/{call_id:uuid}/settlement/refresh', manage, methods=['POST']),
        Route('/v1/accounts/search', manage, methods=['POST']),
        Route("/v1/accounts", manage, methods=["GET", "POST"]),
        Route("/v1/accounts/{account_id:uuid}", manage, methods=["GET", "PATCH", "DELETE"]),
        Route("/v1/accounts/{account_id:uuid}/refresh", manage, methods=["POST"]),
        Route("/v1/accounts/{account_id:uuid}/credential", manage, methods=["PUT"]),
        Route("/v1/overview", manage), Route("/v1/authorization", manage, methods=["GET", "PUT", "DELETE"]),
        Route("/v1/authorization/check", manage, methods=["POST"]),
        Route("/v1/limits", manage, methods=["GET", "PUT"]), Route("/v1/usage", manage)])
    execution = Starlette(routes=[Route("/health", health), Route("/v1/submissions", execute, methods=["POST"]),
        Route('/v1/capabilities', execute, methods=['GET']),
        Route('/v1/cost', execute, methods=['POST']),
        Route('/v1/calls', execute, methods=['POST']),
        Route('/v1/calls/by-idempotency/{operation_id}', execute),
        Route('/v1/calls/{call_id:uuid}', execute),
        Route('/v1/calls/{call_id:uuid}/result', execute),
        Route("/v1/submissions/{submission_id:uuid}", execute),
        Route("/v1/submissions/{submission_id:uuid}/result", execute)])
    management.state.gateway = gateway
    return management, execution


async def main():
    with execution_owner(os.environ.get('FAL_STATE_DB', '/data/fal/state.sqlite3')):
        management, execution = create_apps()
        gateway = management.state.gateway
        lifetime = None
        publication_task = None
        try:
            publisher = configured_publisher(os.environ, gateway.store, __version__)

            class ChannelServer(uvicorn.Server):
                def handle_exit(self, sig, frame):
                    gateway.accepting = False
                    if lifetime:
                        lifetime.begin_draining()
                    super().handle_exit(sig, frame)
                    for server in servers:
                        server.should_exit = True
                        server.force_exit = self.force_exit

            servers = [ChannelServer(uvicorn.Config(app, host="0.0.0.0", port=port, access_log=False))
                       for app, port in ((management, 8000), (execution, 8001))]
            if publisher:
                lifetime = PublicationLifetime(publisher, servers, lambda value: setattr(gateway, 'accepting', value))
                publication_task = asyncio.create_task(lifetime.run())
            def begin_draining():
                gateway.accepting = False
                if lifetime:
                    lifetime.begin_draining()
            await serve_owned_servers(servers, begin_draining)
        finally:
            try:
                if lifetime:
                    await lifetime.finish(publication_task)
            finally:
                gateway.close()


if __name__ == "__main__":
    asyncio.run(main())
