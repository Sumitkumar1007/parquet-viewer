from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class Settings:
    app_name: str
    app_env: str
    secret_key: str
    admin_username: str
    admin_password_hash: str
    parquet_path: Any
    parquet_root: Any
    session_ttl_minutes: int
    max_preview_rows: int
    default_page_size: int
    max_page_size: int
    max_upload_mb: int
    allow_recursive_scan: bool
    users_file: Path
    audit_log_path: Path
    hdfs_base_path: str


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
    parquet_path_raw = os.getenv("PARQUET_PATH", "./data/sample.parquet")
    parquet_path = parquet_path_raw if parquet_path_raw.startswith("hdfs://") else Path(parquet_path_raw).resolve()

    parquet_root_raw = os.getenv("PARQUET_ROOT")
    if parquet_root_raw:
        parquet_root = parquet_root_raw if parquet_root_raw.startswith("hdfs://") else Path(parquet_root_raw).resolve()
    else:
        parquet_root = (
            str(Path(str(parquet_path)).parent)
            if isinstance(parquet_path, Path)
            else str(parquet_path).rsplit("/", 1)[0]
        )
    session_ttl_minutes = int(os.getenv("SESSION_TTL_MINUTES", "480"))
    max_preview_rows = int(os.getenv("MAX_PREVIEW_ROWS", "200"))
    default_page_size = int(os.getenv("DEFAULT_PAGE_SIZE", "50"))
    max_page_size = int(os.getenv("MAX_PAGE_SIZE", "500"))
    max_upload_mb = int(os.getenv("MAX_UPLOAD_MB", "25"))
    allow_recursive_scan = os.getenv("ALLOW_RECURSIVE_SCAN", "false").lower() == "true"
    users_file = Path(os.getenv("USERS_FILE", "./runtime/users.json")).resolve()
    audit_log_path = Path(os.getenv("AUDIT_LOG_PATH", "./runtime/audit.log")).resolve()
    hdfs_base_path = os.getenv("HDFS_BASE_PATH", "").strip().rstrip("/")

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
        default_page_size=default_page_size,
        max_page_size=max_page_size,
        max_upload_mb=max_upload_mb,
        allow_recursive_scan=allow_recursive_scan,
        users_file=users_file,
        audit_log_path=audit_log_path,
        hdfs_base_path=hdfs_base_path,
    )
