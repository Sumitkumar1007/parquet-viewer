from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import secrets
from datetime import UTC, datetime, timedelta
from typing import Any


PBKDF2_ITERATIONS = 600_000


def hash_password(password: str) -> str:
    salt = os.urandom(16)
    digest = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        salt,
        PBKDF2_ITERATIONS,
    )
    return "pbkdf2_sha256${iterations}${salt}${digest}".format(
        iterations=PBKDF2_ITERATIONS,
        salt=base64.urlsafe_b64encode(salt).decode("utf-8"),
        digest=base64.urlsafe_b64encode(digest).decode("utf-8"),
    )


def verify_password(password: str, password_hash: str) -> bool:
    try:
        algorithm, iterations, salt_b64, digest_b64 = password_hash.split("$", 3)
    except ValueError:
        return False

    if algorithm != "pbkdf2_sha256":
        return False

    digest = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        base64.urlsafe_b64decode(salt_b64.encode("utf-8")),
        int(iterations),
    )
    return hmac.compare_digest(
        digest,
        base64.urlsafe_b64decode(digest_b64.encode("utf-8")),
    )


def _sign(secret_key: str, payload: bytes) -> str:
    signature = hmac.new(
        secret_key.encode("utf-8"),
        payload,
        hashlib.sha256,
    ).digest()
    return base64.urlsafe_b64encode(signature).decode("utf-8")


def create_session_token(username: str, secret_key: str, ttl_minutes: int) -> str:
    expires_at = datetime.now(UTC) + timedelta(minutes=ttl_minutes)
    payload = {
        "sub": username,
        "exp": int(expires_at.timestamp()),
        "nonce": secrets.token_urlsafe(12),
    }
    payload_bytes = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    payload_b64 = base64.urlsafe_b64encode(payload_bytes).decode("utf-8")
    signature = _sign(secret_key, payload_b64.encode("utf-8"))
    return f"{payload_b64}.{signature}"


def decode_session_token(token: str, secret_key: str) -> dict[str, Any] | None:
    try:
        payload_b64, signature = token.split(".", 1)
    except ValueError:
        return None

    expected = _sign(secret_key, payload_b64.encode("utf-8"))
    if not hmac.compare_digest(signature, expected):
        return None

    try:
        payload = json.loads(base64.urlsafe_b64decode(payload_b64.encode("utf-8")))
    except (ValueError, json.JSONDecodeError):
        return None

    expires_at = payload.get("exp")
    if not isinstance(expires_at, int):
        return None

    if datetime.now(UTC).timestamp() >= expires_at:
        return None

    return payload
