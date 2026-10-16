"""Deterministic App-owned Dashboard package and verified, bounded asset reads."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import re

from . import __version__


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def decode(data):
    def unique(pairs):
        value = {}
        for key, item in pairs:
            if key in value:
                raise ValueError("INVALID_DASHBOARD_ARTIFACT")
            value[key] = item
        return value
    return json.loads(data, object_pairs_hook=unique)


def build(source, output, version):
    if not re.fullmatch(r"(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)", version):
        raise ValueError("INVALID_APP_VERSION")
    source, output = Path(source), Path(output)
    if output.exists() and any(output.iterdir()):
        raise ValueError("OUTPUT_NOT_EMPTY")
    files = {}

    def asset(name, data):
        original = Path(name)
        path = f"assets/{original.stem}.{hashlib.sha256(data).hexdigest()[:24]}{original.suffix}"
        files[path] = data
        return path

    css = asset("dashboard.css", (source / "dashboard.css").read_bytes())
    code = (source / "dashboard.mjs").read_text(encoding="utf-8")
    entry = asset("dashboard.mjs", code.encode("utf-8"))
    html = (source / "index.html").read_text(encoding="utf-8")
    if html.count("./dashboard.css") != 1 or html.count("./dashboard.mjs") != 1:
        raise ValueError("INVALID_DASHBOARD_ENTRY")
    files["index.html"] = html.replace("./dashboard.css", "./" + css).replace("./dashboard.mjs", "./" + entry).encode("utf-8")
    index = canonical({"schemaVersion": 1, "files": {name: hashlib.sha256(data).hexdigest() for name, data in sorted(files.items())}})
    declaration = {"schemaVersion": 1, "appId": "fal", "appVersion": version, "dashboard": {
        "entryRef": "dashboard/index.html", "artifactDigest": "sha256:" + hashlib.sha256(index).hexdigest(),
        "adapter": "wujie", "sdkMajor": 1, "minimumSdkVersion": "1.2.0",
        "requiredCapabilities": ["app.request", "navigation.request"], "optionalCapabilities": ["navigation.openApp"],
        "appApiPolicyRef": "fal-management-v1", "keepAlive": True}}
    output.mkdir(parents=True, exist_ok=True)
    for name, data in files.items():
        target = output / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)
    (output / "asset-index.json").write_bytes(index)
    (output / "dashboard-declaration.json").write_bytes(canonical(declaration))
    DashboardAssets(output, version=version)
    return declaration


class DashboardAssets:
    TYPES = {".html": "text/html; charset=utf-8", ".mjs": "text/javascript; charset=utf-8", ".css": "text/css; charset=utf-8"}

    def __init__(self, root, *, version=__version__):
        self.root = Path(root).resolve()
        self.declaration = self._read("dashboard-declaration.json", 65536)
        self.index = self._read("asset-index.json", 65536)
        declaration, index = decode(self.declaration), decode(self.index)
        if (not isinstance(declaration, dict) or set(declaration) != {"schemaVersion", "appId", "appVersion", "dashboard"}
                or type(declaration["schemaVersion"]) is not int or declaration["schemaVersion"] != 1
                or declaration["appId"] != "fal" or declaration["appVersion"] != version):
            raise ValueError("INVALID_DASHBOARD_ARTIFACT")
        dashboard = declaration["dashboard"]
        if (not isinstance(dashboard, dict) or dashboard.get("adapter") != "wujie"
                or dashboard.get("entryRef") != "dashboard/index.html"
                or dashboard.get("appApiPolicyRef") != "fal-management-v1"
                or dashboard.get("artifactDigest") != "sha256:" + hashlib.sha256(self.index).hexdigest()
                or not isinstance(index, dict) or set(index) != {"schemaVersion", "files"}
                or type(index["schemaVersion"]) is not int or index["schemaVersion"] != 1
                or not isinstance(index["files"], dict) or not 1 <= len(index["files"]) <= 256
                or "index.html" not in index["files"]):
            raise ValueError("INVALID_DASHBOARD_ARTIFACT")
        self.files = dict(index["files"])
        for name, digest in self.files.items():
            if (not self.safe(name) or Path(name).suffix not in self.TYPES
                    or not isinstance(digest, str) or not re.fullmatch("[a-f0-9]{64}", digest)):
                raise ValueError("INVALID_DASHBOARD_ARTIFACT")
            self.asset(name)

    @staticmethod
    def safe(name):
        return (isinstance(name, str) and len(name) <= 256
                and re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9._/-]*", name)
                and all(part and not part.startswith(".") for part in name.split("/")))

    def _read(self, name, maximum=4 << 20):
        if not self.safe(name):
            raise ValueError("INVALID_DASHBOARD_PATH")
        target = self.root / name
        if (any(path.is_symlink() for path in [target, *target.parents] if path != self.root)
                or not target.resolve().is_relative_to(self.root) or not target.is_file()
                or target.stat().st_size > maximum):
            raise ValueError("DASHBOARD_ASSET_UNAVAILABLE")
        with target.open("rb") as handle:
            data = handle.read(maximum + 1)
        if len(data) > maximum:
            raise ValueError("DASHBOARD_ASSET_UNAVAILABLE")
        return data

    def asset(self, name):
        if name not in self.files:
            raise KeyError("NOT_FOUND")
        data = self._read(name)
        if hashlib.sha256(data).hexdigest() != self.files[name]:
            raise ValueError("DASHBOARD_ASSET_MISMATCH")
        return data, self.TYPES[Path(name).suffix]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    build(args.source, args.output, __version__)
