"""Safe supplier diagnostics: retain known classifications, never raw messages."""
from __future__ import annotations


REASONS = {
    'provider_invalid_request': ('fal rejected the request parameters', False),
    'provider_auth_failed': ('fal rejected the account credentials or permissions', False),
    'provider_model_unavailable': ('fal could not find the requested model endpoint', False),
    'provider_rate_limited': ('fal rate limit was reached; retain the original attempt', True),
    'provider_quota_exhausted': ('fal account has insufficient credits or quota', False),
    'provider_content_rejected': ('fal rejected the request under its content rules', False),
    'provider_failed': ('fal reported that this generation failed', False),
    'provider_unavailable': ('fal is temporarily unavailable', True),
    'proxy_unavailable': ('the configured channel proxy is unavailable', True),
}
PROVIDER_CODES = {
    'authorization_error': 'provider_auth_failed',
    'validation_error': 'provider_invalid_request',
    'not_found': 'provider_model_unavailable',
    'rate_limited': 'provider_rate_limited',
    'server_error': 'provider_unavailable',
    'insufficient_credits': 'provider_quota_exhausted',
    'insufficient_balance': 'provider_quota_exhausted',
    'quota_exceeded': 'provider_quota_exhausted',
    'content_policy_violation': 'provider_content_rejected',
    'content_policy_violation_error': 'provider_content_rejected',
}
INPUT_FIELDS = {'prompt', 'duration', 'resolution', 'aspect_ratio', 'generate_audio',
                'image_urls', 'video_urls', 'audio_urls', 'reference_image_urls',
                'reference_video_urls', 'reference_audio_urls', 'enable_safety_checker',
                'sync_mode', 'prompt_expansion_mode'}


def failure(code, *, accepted, provider_code=None, fields=()):
    message, retryable = REASONS[code]
    if fields:
        message += ': ' + ', '.join(sorted(fields))
    return {'code': code, 'message': message, 'retryable': retryable,
            'source': 'fal', 'accepted': accepted, 'provider_code': provider_code}


def provider_failure(data, status=None, *, accepted):
    code = {400: 'provider_invalid_request', 401: 'provider_auth_failed',
            402: 'provider_quota_exhausted', 403: 'provider_auth_failed',
            404: 'provider_model_unavailable', 422: 'provider_invalid_request',
            429: 'provider_rate_limited'}.get(status, 'provider_failed' if status is None else 'provider_unavailable')
    provider_code, fields = None, set()
    if isinstance(data, dict):
        error = data.get('error')
        candidates = [data.get('error_type'), data.get('code')]
        if isinstance(error, dict):
            candidates.extend([error.get('type'), error.get('code')])
        for candidate in candidates:
            if isinstance(candidate, str) and candidate.lower() in PROVIDER_CODES:
                provider_code = candidate.lower()
                code = PROVIDER_CODES[provider_code]
                break
        details = data.get('detail')
        if isinstance(details, list):
            for detail in details:
                loc = detail.get('loc') if isinstance(detail, dict) else None
                if isinstance(loc, list):
                    fields.update(value for value in loc if isinstance(value, str) and value in INPUT_FIELDS)
    return failure(code, accepted=accepted, provider_code=provider_code, fields=fields)
