"""Reviewed estimates and read-only, request-bound fal billing evidence."""
from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
import hashlib
import base64
import binascii
import io
import json
import wave

import httpx


H3 = 'minimax/h3/reference-to-video'
SD2 = 'bytedance/seedance-2.0/reference-to-video'
LIP = 'minimax/h3-max/lip-sync/image-to-video'
BILLING_URL = 'https://api.fal.ai/v1/models/billing-events'
REVIEWED_AT = datetime(2026, 10, 27, tzinfo=UTC)
VALID_UNTIL = REVIEWED_AT + timedelta(days=30)
PRICE_VERSION = 'fal-video-20261027'


class BillingError(ValueError):
    def __init__(self, code, status=409):
        self.code, self.status = code, status
        super().__init__(code)


def input_digest(model, payload):
    return hashlib.sha256(json.dumps([model, payload], sort_keys=True,
        separators=(',', ':'), ensure_ascii=False).encode()).hexdigest()


def estimate(model, payload, *, at=None):
    observed = at or datetime.now(UTC)
    if observed >= VALID_UNTIL:
        raise BillingError('pricing_review_required')
    source = 'https://fal.ai/models/' + model
    value = {'schema_version': 1, 'model_id': model,
             'input_digest': input_digest(model, payload),
             'price_version': PRICE_VERSION, 'reviewed_at': REVIEWED_AT.isoformat(),
             'valid_until': VALID_UNTIL.isoformat(), 'quoted_at': observed.isoformat(),
             'source_url': source, 'estimate': {'amount': None, 'unit': 'USD', 'source': 'unknown'}}
    if model == H3:
        if payload.get('resolution') != '480P':
            raise BillingError('pricing_profile_not_supported')
        extra_images = max(len(payload.get('reference_image_urls', [])) - 5, 0)
        total = Decimal(payload['duration']) * Decimal('0.05') + extra_images * Decimal('0.08')
        value.update(estimate={'amount': format(total, 'f'), 'unit': 'USD', 'source': 'official_price_table'},
                     rates={'output_second': '0.05', 'extra_image': '0.08', 'free_images': 5},
                     reason_code=None)
    elif model == LIP and payload.get('resolution') in {'480P', '768P'}:
        # The first VF adapter accepts PCM WAV, so its quantity is measured from
        # the submitted bytes rather than an unrelated caller duration field.
        try:
            uri = payload['audio_url']
            if not uri.startswith('data:audio/wav;base64,'):
                raise ValueError()
            blob = base64.b64decode(uri.split(',', 1)[1], validate=True)
            with wave.open(io.BytesIO(blob), 'rb') as audio:
                frames, rate = audio.getnframes(), audio.getframerate()
                if audio.getcomptype() != 'NONE' or not 5 * rate <= frames <= 15 * rate:
                    raise ValueError()
                if len(audio.readframes(frames)) != frames * audio.getnchannels() * audio.getsampwidth():
                    raise ValueError()
            seconds = Decimal(frames) / Decimal(rate)
        except (KeyError, TypeError, AttributeError, ValueError, EOFError, wave.Error, binascii.Error):
            raise BillingError('lip_sync_pcm_wav_required', 400) from None
        unit_price = Decimal('0.05' if payload['resolution'] == '480P' else '0.08')
        value.update(price_version='fal-lip-sync-20261028',
                     reviewed_at=datetime(2026, 10, 28, tzinfo=UTC).isoformat(),
                     estimate={'amount': format(seconds * unit_price, 'f'), 'unit': 'USD',
                               'source': 'official_price_table'},
                     rates={'output_second': str(unit_price), 'audio_seconds': str(seconds)}, reason_code=None)
    elif model == SD2 and payload.get('resolution') in {'480p', '720p'}:
        video_reference = bool(payload.get('video_urls'))
        value.update(rates={'video_tokens_1000': '0.0086016' if video_reference else '0.014',
                            'video_reference': video_reference},
                     reason_code='provider_usage_required')
    else:
        raise BillingError('pricing_profile_not_supported')
    return value


def _decimal(value):
    if isinstance(value, bool) or not isinstance(value, (str, int, Decimal)):
        raise BillingError('billing_response_invalid', 502)
    try:
        result = Decimal(value)
    except InvalidOperation:
        raise BillingError('billing_response_invalid', 502) from None
    if not result.is_finite() or result < 0 or result > Decimal('1e24'):
        raise BillingError('billing_response_invalid', 502)
    return result


def fetch_bill(client, key, call, *, at=None):
    """Never submit generation or accept a caller-provided amount/remote URL."""
    remote = call.get('provider_request_id')
    if not remote or call.get('accepted') != 1:
        raise BillingError('billing_request_unconfirmed')
    if call.get('state') not in {'completed', 'failed'}:
        raise BillingError('billing_task_not_terminal')
    observed = at or datetime.now(UTC)
    try:
        created = datetime.fromisoformat(call['created_at'])
        if created.tzinfo is None or created > observed:
            raise ValueError()
    except (ValueError, TypeError):
        raise BillingError('billing_date_invalid') from None
    start = created - timedelta(days=1)
    if observed - start > timedelta(days=90):
        raise BillingError('billing_window_exceeded')
    params = {'endpoint_id': call['model_id'], 'request_id': remote, 'source': 'billed',
              'start': start.isoformat(), 'end': observed.isoformat(), 'limit': 100}
    cursors, events = set(), []
    for _ in range(100):
        try:
            response = client.get(BILLING_URL, params=params,
                                  headers={'Authorization': 'Key ' + key}, timeout=20)
        except httpx.HTTPError:
            raise BillingError('billing_unavailable', 503) from None
        if response.status_code in {401, 403}:
            raise BillingError('billing_access_denied', 403)
        if response.status_code == 429:
            raise BillingError('billing_rate_limited', 429)
        if not response.is_success:
            raise BillingError('billing_unavailable', 503)
        if len(response.content) > 2 * 1024 * 1024:
            raise BillingError('billing_response_invalid', 502)
        try:
            data = response.json(parse_float=Decimal)
        except ValueError:
            raise BillingError('billing_response_invalid', 502) from None
        if (not isinstance(data, dict) or not isinstance(data.get('billing_events'), list)
                or type(data.get('has_more')) is not bool or 'next_cursor' not in data):
            raise BillingError('billing_response_invalid', 502)
        for row in data['billing_events']:
            if not isinstance(row, dict) or row.get('request_id') != remote or row.get('endpoint_id') != call['model_id']:
                raise BillingError('billing_identity_mismatch', 502)
            try:
                timestamp = datetime.fromisoformat(row['timestamp'])
                if timestamp.tzinfo is None or not start <= timestamp < observed:
                    raise ValueError()
            except (KeyError, TypeError, ValueError):
                raise BillingError('billing_response_invalid', 502) from None
            nano = _decimal(row.get('cost_estimate_nano_usd'))
            total = _decimal(row.get('cost_total'))
            if nano != nano.to_integral_value() or total != nano / Decimal(1_000_000_000):
                raise BillingError('billing_amount_mismatch', 502)
            # The documented endpoint reports USD; reject unexpected currency extensions.
            if row.get('currency', 'USD') != 'USD':
                raise BillingError('billing_currency_mismatch', 502)
            events.append({'request_id': remote, 'endpoint_id': call['model_id'],
                           'timestamp': timestamp.astimezone(UTC).isoformat(),
                           'amount_nano_usd': str(int(nano))})
        cursor = data['next_cursor']
        if not data['has_more']:
            if cursor is not None:
                raise BillingError('billing_pagination_invalid', 502)
            break
        if not isinstance(cursor, str) or not 1 <= len(cursor) <= 2048 or cursor in cursors:
            raise BillingError('billing_pagination_invalid', 502)
        cursors.add(cursor)
        params['cursor'] = cursor
    else:
        raise BillingError('billing_pagination_incomplete', 502)
    if not events:
        raise BillingError('billing_record_not_found')
    # Without a supplier event identifier, multiple records cannot safely be summed
    # or deduplicated: a duplicated page must never reduce or double the real charge.
    if len(events) != 1:
        raise BillingError('billing_record_ambiguous', 502)
    event = events[0]
    encoded = json.dumps(event, sort_keys=True, separators=(',', ':'))
    return {'amount': format(Decimal(event['amount_nano_usd']) / Decimal(1_000_000_000), 'f'),
            'source': 'fal_billing_events', 'evidence_digest': hashlib.sha256(encoded.encode()).hexdigest(),
            'evidence_json': encoded, 'event_count': 1, 'observed_at': observed.isoformat()}
