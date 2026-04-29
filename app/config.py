from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    app_name: str
    app_env: str
    secret_key: str
    admin_username: str
    admin_password_hash: str
    parquet_path: Path
    parquet_root: Path
    session_ttl_minutes: int
    max_preview_rows: int


def _load_dotenv() -> None:
    env_path = Path(".env")
    if not env_path.exists():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())


def get_settings() -> Settings:
    _load_dotenv()

    app_name = os.getenv("APP_NAME", "Parquet Query Studio")
    app_env = os.getenv("APP_ENV", "development")
    secret_key = os.getenv("SECRET_KEY", "dev-secret-change-me")
    admin_username = os.getenv("ADMIN_USERNAME", "admin")
    admin_password_hash = os.getenv("ADMIN_PASSWORD_HASH", "")
    parquet_path = Path(os.getenv("PARQUET_PATH", "./data/sample.parquet")).resolve()
    parquet_root = Path(os.getenv("PARQUET_ROOT", str(parquet_path.parent))).resolve()
    session_ttl_minutes = int(os.getenv("SESSION_TTL_MINUTES", "480"))
    max_preview_rows = int(os.getenv("MAX_PREVIEW_ROWS", "200"))

    return Settings(
        app_name=app_name,
        app_env=app_env,
        secret_key=secret_key,
        admin_username=admin_username,
        admin_password_hash=admin_password_hash,
        parquet_path=parquet_path,
        parquet_root=parquet_root,
        session_ttl_minutes=session_ttl_minutes,
        max_preview_rows=max_preview_rows,
    )
