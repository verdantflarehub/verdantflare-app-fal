"""Validated, read-only channel metadata from the mounted Chart declaration."""
from __future__ import annotations

import copy
import hashlib
import re
from pathlib import Path
from urllib.parse import urlsplit

import yaml


_IDENTIFIER = re.compile(r"[a-z0-9][a-z0-9._-]{0,127}\Z")
_MODEL = re.compile(r"[a-z0-9][a-z0-9._/-]{0,191}\Z")
_RESOLUTION = re.compile(r"(?:[1-9][0-9]*[pP]|[24]K)\Z")
_EXISTING_EXECUTION = {
    "minimax/h3/reference-to-video",
    "bytedance/seedance-2.0/reference-to-video",
    "minimax/h3-max/lip-sync/image-to-video",
}


class _UniqueLoader(yaml.SafeLoader):
    def compose_node(self, parent, index):
        if self.check_event(yaml.AliasEvent):
            raise ValueError("channel_metadata_alias")
        return super().compose_node(parent, index)

    def construct_mapping(self, node, deep=False):
        result = {}
        for key_node, value_node in node.value:
            key = self.construct_object(key_node, deep=deep)
            if not isinstance(key, str) or key in result:
                raise ValueError("channel_metadata_duplicate_key")
            result[key] = self.construct_object(value_node, deep=deep)
        return result


def load_metadata(path: str | Path) -> dict:
    raw = Path(path).read_bytes()
    if not raw or len(raw) > 256 * 1024:
        raise ValueError("channel_metadata_size")
    try:
        data = yaml.load(raw.decode("utf-8"), Loader=_UniqueLoader)
    except (UnicodeDecodeError, yaml.YAMLError) as error:
        raise ValueError("channel_metadata_invalid") from error
    if not isinstance(data, dict) or set(data) != {"schema_version", "channel_id", "capabilities"}:
        raise ValueError("channel_metadata_shape")
    if data["schema_version"] != 1 or data["channel_id"] != "fal":
        raise ValueError("channel_metadata_identity")
    items = data["capabilities"]
    if not isinstance(items, list) or not 1 <= len(items) <= 64:
        raise ValueError("channel_metadata_capabilities")
    seen_ids, seen_endpoints = set(), set()
    required = {"capability_id", "type", "model_id", "endpoint_id", "display_name",
                "operation", "parameter_schema", "execution_state"}
    optional = {"protocol", "output_resolutions", "source_url", "card_group",
                "card_title", "input_summary", "output_summary"}
    for item in items:
        if not isinstance(item, dict) or not required <= set(item) or set(item) - required - optional:
            raise ValueError("channel_metadata_entry")
        cap, model, endpoint = (item[name] for name in ("capability_id", "model_id", "endpoint_id"))
        if (not isinstance(cap, str) or not _IDENTIFIER.fullmatch(cap)
                or not isinstance(model, str) or not _MODEL.fullmatch(model)
                or not isinstance(endpoint, str) or not _MODEL.fullmatch(endpoint)
                or not endpoint.startswith(model + "/") or cap in seen_ids or endpoint in seen_endpoints):
            raise ValueError("channel_metadata_endpoint")
        seen_ids.add(cap)
        seen_endpoints.add(endpoint)
        if (item["type"] != "video" or not isinstance(item["display_name"], str)
                or not 1 <= len(item["display_name"]) <= 100
                or not isinstance(item["operation"], str)
                or not _IDENTIFIER.fullmatch(item["operation"])
                or not isinstance(item["parameter_schema"], dict)
                or not isinstance(item["execution_state"], str)
                or item["execution_state"] not in {"catalog_only", "adapter_connected_unverified"}):
            raise ValueError("channel_metadata_fields")
        if item["execution_state"] != "catalog_only" and endpoint not in _EXISTING_EXECUTION:
            raise ValueError("channel_metadata_execution_claim")
        if "protocol" in item and (not isinstance(item["protocol"], str)
                                   or item["protocol"] not in {"queue", "websocket"}):
            raise ValueError("channel_metadata_protocol")
        if "output_resolutions" in item:
            values = item["output_resolutions"]
            if (not isinstance(values, list) or not values or len(values) > 12
                    or any(not isinstance(value, str) or not _RESOLUTION.fullmatch(value) for value in values)
                    or len(values) != len(set(values))):
                raise ValueError("channel_metadata_resolutions")
        if "source_url" in item:
            if not isinstance(item["source_url"], str):
                raise ValueError("channel_metadata_source")
            url = urlsplit(item["source_url"])
            expected = {"/models/" + endpoint + "/api"}
            if model == "minimax/h3":
                expected.add("/minimax-h3")
            if (url.scheme != "https" or url.hostname != "fal.ai" or url.port not in {None, 443}
                    or url.username or url.password
                    or url.query or url.fragment or url.path not in expected):
                raise ValueError("channel_metadata_source")
        card_fields = {"card_group", "card_title", "input_summary", "output_summary"}
        if card_fields.intersection(item) and not card_fields <= set(item):
            raise ValueError("channel_metadata_card_incomplete")
        if card_fields <= set(item):
            if (not isinstance(item["card_group"], str)
                    or not _IDENTIFIER.fullmatch(item["card_group"])
                    or any(not isinstance(item[field], str) or not 1 <= len(item[field]) <= 160
                           for field in ("card_title", "input_summary", "output_summary"))):
                raise ValueError("channel_metadata_card")
    return {"schema_version": 1, "channel_id": "fal", "version": hashlib.sha256(raw).hexdigest(),
            "capabilities": copy.deepcopy(items)}
