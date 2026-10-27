import json
import unittest

import httpx

from fal_gateway.errors import provider_failure
import test_calls


class ProviderErrorTest(unittest.TestCase):
    def test_known_reason_and_fields_survive_without_raw_messages(self):
        result = provider_failure({'error': {'type': 'validation_error', 'message': 'secret signed URL'},
            'detail': [{'loc': ['body', 'duration'], 'msg': 'private'}, {'loc': ['body', 'secret-key']}]},
            422, accepted=False)
        self.assertEqual(result['code'], 'provider_invalid_request')
        self.assertEqual(result['provider_code'], 'validation_error')
        self.assertIn('duration', result['message'])
        for private in ('secret', 'private', 'URL'):
            self.assertNotIn(private, json.dumps(result))

    def test_rejection_reason_is_durable_and_replay_never_submits(self):
        f = test_calls.CallsTest()
        f.setUp()
        try:
            f.mode = 'reject'
            first = f.submit().json()
            self.assertEqual(first['reason_code'], 'provider_rate_limited')
            self.assertFalse(first['accepted'])
            self.assertEqual(first['error']['source'], 'fal')
            self.assertEqual(f.submit().json(), first)
            self.assertEqual(len(f.sent), 1)
            self.assertEqual(f.gateway.calls.list('org')['items'][0]['error'], first['error'])
        finally:
            f.tearDown()

    def test_terminal_error_is_persisted_but_poll_errors_do_not_change_acceptance(self):
        f = test_calls.CallsTest()
        f.setUp()
        try:
            call_id = f.submit().json()['call_id']
            def upstream(request):
                return httpx.Response(200, json={'status': 'COMPLETED',
                    'error_type': 'content_policy_violation', 'error': 'raw secret details'})
            f.gateway.client.close()
            f.gateway.client = httpx.Client(transport=httpx.MockTransport(upstream))
            result = f.client.get('/v1/calls/' + call_id, headers=f.headers)
            self.assertEqual(result.json()['state'], 'failed')
            self.assertEqual(result.json()['reason_code'], 'provider_content_rejected')
            self.assertTrue(result.json()['accepted'])
            self.assertNotIn('raw secret', result.text)
            f.gateway.client.close()
            f.gateway.client = httpx.Client(transport=httpx.MockTransport(lambda request:
                httpx.Response(429, json={'error': {'type': 'rate_limited', 'message': 'secret'}})))
            response = f.client.get('/v1/calls/' + call_id, headers=f.headers)
            self.assertEqual(response.status_code, 429)
            self.assertEqual(response.json()['error'], 'provider_rate_limited')
            self.assertTrue(response.json()['diagnostic']['accepted'])
            self.assertTrue(f.gateway.calls.get('org', call_id)['accepted'])
        finally:
            f.tearDown()


if __name__ == '__main__':
    unittest.main()
