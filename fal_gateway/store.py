"""Encrypted organization credentials and durable submission identity."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import sqlite3
from datetime import UTC, datetime
from contextlib import contextmanager

from cryptography.fernet import Fernet


def now() -> str:
    return datetime.now(UTC).isoformat()


class Conflict(ValueError):
    pass


class Store:
    def __init__(self, path: str, encryption_key: str):
        if not encryption_key:
            raise ValueError("FAL_ENCRYPTION_KEY is required")
        self.cipher = Fernet(encryption_key.encode())
        self.operation_key = hashlib.sha256(b"channel-account-operations:" + encryption_key.encode()).digest()
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.path.touch(mode=0o600, exist_ok=True)
        os.chmod(self.path, 0o600)
        with self.connect() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS organizations (
                  organization_id TEXT PRIMARY KEY, active_version INTEGER,
                  enabled INTEGER NOT NULL DEFAULT 0,
                  daily_limit INTEGER NOT NULL DEFAULT 0,
                  revision INTEGER NOT NULL DEFAULT 0
                );
                CREATE TABLE IF NOT EXISTS credentials (
                  organization_id TEXT NOT NULL, version INTEGER NOT NULL,
                  ciphertext BLOB NOT NULL, state TEXT NOT NULL,
                  checked_at TEXT, created_at TEXT NOT NULL,
                  PRIMARY KEY (organization_id, version)
                );
                CREATE TABLE IF NOT EXISTS submissions (
                  id TEXT PRIMARY KEY, organization_id TEXT NOT NULL,
                  operation_id TEXT NOT NULL, digest TEXT NOT NULL,
                  credential_version INTEGER NOT NULL, model_id TEXT NOT NULL,
                  provider_request_id TEXT, state TEXT NOT NULL,
                  created_at TEXT NOT NULL,
                  UNIQUE (organization_id, operation_id)
                );
                CREATE TABLE IF NOT EXISTS channel_metadata (
                  name TEXT PRIMARY KEY, value TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS channel_legacy_accounts (
                  organization_id TEXT PRIMARY KEY, account_id TEXT NOT NULL,
                  migrated_at TEXT NOT NULL
                );
            """)

    @contextmanager
    def connect(self):
        db = sqlite3.connect(self.path, timeout=30)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA busy_timeout=30000")
        try:
            with db:
                yield db
        finally:
            db.close()

    def overview(self, organization_id: str) -> dict:
        with self.connect() as db:
            org = db.execute("SELECT * FROM organizations WHERE organization_id=?", (organization_id,)).fetchone()
            if not org:
                return {"status": "unconfigured", "enabled": False, "daily_limit": 0, "revision": 0,
                        "checked_at": None, "version": None}
            credential = db.execute("SELECT state,checked_at FROM credentials WHERE organization_id=? AND version=?",
                                    (organization_id, org["active_version"])).fetchone() if org["active_version"] else None
            return {"status": credential["state"] if credential else "unconfigured",
                    "enabled": bool(org["enabled"]), "daily_limit": org["daily_limit"],
                    "revision": org["revision"], "checked_at": credential["checked_at"] if credential else None,
                    "version": org["active_version"]}

    @staticmethod
    def legacy_writable(db):
        if db.execute("SELECT 1 FROM channel_metadata WHERE name='accounts_cutover'").fetchone():
            raise Conflict('channel_accounts_required')

    def candidate(self, organization_id: str, key: str, expected_revision: int) -> int:
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            self.legacy_writable(db)
            org = db.execute("SELECT revision FROM organizations WHERE organization_id=?", (organization_id,)).fetchone()
            actual = org["revision"] if org else 0
            if actual != expected_revision:
                raise Conflict("revision_conflict")
            db.execute("INSERT OR IGNORE INTO organizations(organization_id) VALUES (?)", (organization_id,))
            version = db.execute("SELECT COALESCE(MAX(version),0)+1 FROM credentials WHERE organization_id=?",
                                 (organization_id,)).fetchone()[0]
            db.execute("INSERT INTO credentials VALUES (?,?,?,?,?,?)",
                       (organization_id, version, self.cipher.encrypt(key.encode()), "pending_verification", None, now()))
            db.commit()
            return version

    def activate(self, organization_id: str, version: int, expected_revision: int) -> dict:
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            self.legacy_writable(db)
            org = db.execute("SELECT revision,active_version FROM organizations WHERE organization_id=?", (organization_id,)).fetchone()
            if not org or org["revision"] != expected_revision:
                raise Conflict("revision_conflict")
            candidate = db.execute("SELECT state FROM credentials WHERE organization_id=? AND version=?",
                                   (organization_id, version)).fetchone()
            if not candidate or candidate["state"] != "pending_verification":
                raise Conflict("credential_state_conflict")
            db.execute("UPDATE credentials SET state='active',checked_at=? WHERE organization_id=? AND version=?",
                       (now(), organization_id, version))
            db.execute("UPDATE organizations SET active_version=?,revision=revision+1 WHERE organization_id=?",
                       (version, organization_id))
            db.commit()
        return self.overview(organization_id)

    def mark_candidate(self, organization_id: str, version: int, state: str):
        if state not in {"invalid", "unavailable"}:
            raise ValueError("invalid_state")
        with self.connect() as db:
            db.execute("UPDATE credentials SET state=?,checked_at=? WHERE organization_id=? AND version=? AND state='pending_verification'",
                       (state, now(), organization_id, version))

    def decrypt(self, organization_id: str, version: int) -> str:
        with self.connect() as db:
            row = db.execute("SELECT ciphertext FROM credentials WHERE organization_id=? AND version=?",
                             (organization_id, version)).fetchone()
        if not row:
            raise KeyError("credential_not_found")
        return self.cipher.decrypt(row["ciphertext"]).decode()

    def recheck(self, organization_id: str, state: str) -> dict:
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            self.legacy_writable(db)
            org = db.execute("SELECT active_version FROM organizations WHERE organization_id=?",
                             (organization_id,)).fetchone()
            if not org or not org["active_version"]:
                raise Conflict("authorization_required")
            if state == "invalid":
                db.execute("UPDATE organizations SET enabled=0,revision=revision+1 WHERE organization_id=?",
                           (organization_id,))
            db.execute("UPDATE credentials SET state=?,checked_at=? WHERE organization_id=? AND version=?",
                       (state, now(), organization_id, org["active_version"]))
            db.commit()
        return self.overview(organization_id)

    def usage(self, organization_id: str) -> dict:
        with self.connect() as db:
            rows = db.execute("SELECT state,COUNT(*) AS count FROM submissions WHERE organization_id=? GROUP BY state",
                              (organization_id,)).fetchall()
        return {"local_submissions": {row["state"]: row["count"] for row in rows},
                "provider_billing": "unavailable", "currency": None}

    def set_limits(self, organization_id: str, enabled: bool, daily_limit: int, expected_revision: int) -> dict:
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            self.legacy_writable(db)
            org = db.execute("SELECT revision,active_version FROM organizations WHERE organization_id=?", (organization_id,)).fetchone()
            if not org or org["revision"] != expected_revision:
                raise Conflict("revision_conflict")
            if enabled and not org["active_version"]:
                raise Conflict("authorization_required")
            db.execute("UPDATE organizations SET enabled=?,daily_limit=?,revision=revision+1 WHERE organization_id=?",
                       (int(enabled), daily_limit, organization_id))
            db.commit()
        return self.overview(organization_id)

    def revoke(self, organization_id: str, expected_revision: int) -> dict:
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            self.legacy_writable(db)
            org = db.execute("SELECT revision,active_version FROM organizations WHERE organization_id=?", (organization_id,)).fetchone()
            if not org or org["revision"] != expected_revision:
                raise Conflict("revision_conflict")
            db.execute("UPDATE organizations SET active_version=NULL,enabled=0,revision=revision+1 WHERE organization_id=?",
                       (organization_id,))
            if org["active_version"]:
                db.execute("UPDATE credentials SET state='revoked' WHERE organization_id=? AND version=?",
                           (organization_id, org["active_version"]))
            db.commit()
        return self.overview(organization_id)

    def reserve_submission(self, organization_id: str, operation_id: str, model_id: str,
                           payload: dict, submission_id: str) -> tuple[dict, bool]:
        canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        digest = hashlib.sha256((model_id + "\n" + canonical).encode()).hexdigest()
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            existing = db.execute("SELECT * FROM submissions WHERE organization_id=? AND operation_id=?",
                                  (organization_id, operation_id)).fetchone()
            if existing:
                if existing["digest"] != digest:
                    raise Conflict("idempotency_conflict")
                return dict(existing), False
            self.legacy_writable(db)
            org = db.execute("SELECT active_version,enabled,daily_limit FROM organizations WHERE organization_id=?",
                             (organization_id,)).fetchone()
            if not org or not org["active_version"] or not org["enabled"] or org["daily_limit"] < 1:
                raise Conflict("channel_not_available")
            credential = db.execute("SELECT state FROM credentials WHERE organization_id=? AND version=?",
                                    (organization_id, org["active_version"])).fetchone()
            if not credential or credential["state"] != "active":
                raise Conflict("authorization_not_verified")
            today = datetime.now(UTC).date().isoformat()
            count = db.execute("SELECT COUNT(*) FROM submissions WHERE organization_id=? AND created_at>=?",
                               (organization_id, today)).fetchone()[0]
            if count >= org["daily_limit"]:
                raise Conflict("daily_limit_reached")
            db.execute("INSERT INTO submissions VALUES (?,?,?,?,?,?,?,?,?)",
                       (submission_id, organization_id, operation_id, digest, org["active_version"],
                        model_id, None, "submission_unconfirmed", now()))
            db.commit()
            return dict(db.execute("SELECT * FROM submissions WHERE id=?", (submission_id,)).fetchone()), True

    def submission(self, organization_id: str, submission_id: str) -> dict:
        with self.connect() as db:
            row = db.execute("SELECT * FROM submissions WHERE organization_id=? AND id=?",
                             (organization_id, submission_id)).fetchone()
        if not row:
            raise KeyError("submission_not_found")
        return dict(row)

    def confirm_submission(self, submission_id: str, provider_request_id: str):
        with self.connect() as db:
            db.execute("UPDATE submissions SET provider_request_id=?,state='queued' WHERE id=? AND state='submission_unconfirmed'",
                       (provider_request_id, submission_id))

    def reject_submission(self, submission_id: str):
        with self.connect() as db:
            db.execute("UPDATE submissions SET state='rejected' WHERE id=? AND state='submission_unconfirmed'",
                       (submission_id,))
