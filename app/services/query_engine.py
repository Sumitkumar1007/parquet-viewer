from __future__ import annotations

import hashlib
import subprocess
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from shutil import move
from typing import Any, Optional

import duckdb


ALL_FILES_TOKEN = "__ALL_PARQUET_FILES__"
ALLOWED_PREFIXES = ("select", "with", "describe", "show", "explain")
BLOCKED_TOKENS = (
    "insert",
    "update",
    "delete",
    "drop",
    "alter",
    "create",
    "replace",
    "truncate",
    "attach",
    "detach",
    "copy",
    "export",
    "install",
    "load",
    "call",
)


class QueryValidationError(ValueError):
    """Raised when query is unsafe or unsupported."""


@dataclass
class QueryResult:
    columns: list[str]
    rows: list[list[object]]
    row_count: int
    total_rows: int
    page: int
    page_size: int
    total_pages: int
    elapsed_ms: float
    query: str


class QueryEngine:
    def __init__(
        self,
        parquet_path: Path,
        parquet_root: Path,
        max_preview_rows: int,
        default_page_size: int,
        max_page_size: int,
        max_upload_mb: int,
        allow_recursive_scan: bool,
    ) -> None:
        self.parquet_path = parquet_path
        self.parquet_root = parquet_root
        self.max_preview_rows = max_preview_rows
        self.default_page_size = default_page_size
        self.max_page_size = max_page_size
        self.max_upload_mb = max_upload_mb
        self.allow_recursive_scan = allow_recursive_scan

    def ensure_ready(self, parquet_path: Optional[Path] = None) -> Path:
        target_path = parquet_path or self.parquet_path
        if not target_path.exists():
            raise FileNotFoundError(
                f"Parquet file not found: {target_path}. "
                "Set PARQUET_PATH or generate sample data."
            )
        return target_path

    def ensure_hdfs_dependency(self) -> None:
        try:
            subprocess.run(
                ["hdfs", "dfs", "-ls", "/"],
                check=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        except FileNotFoundError as exc:  # pragma: no cover - environment driven
            raise QueryValidationError(
                "HDFS support requires the 'hdfs' CLI to be installed and available in PATH."
            ) from exc

    def list_files(
        self,
        root_path: Optional[str] = None,
        recursive: Optional[bool] = None,
    ) -> dict[str, object]:
        scan_recursive = self._resolve_recursive(recursive)
        if self._is_hdfs_uri(root_path):
            resolved_root = self.resolve_root(root_path)
            items = self._list_hdfs_files(str(resolved_root), scan_recursive)
            return {"root_path": str(resolved_root), "items": items, "recursive": scan_recursive}

        resolved_root = self.resolve_root(root_path)
        resolved_root.mkdir(parents=True, exist_ok=True)
        files = sorted(resolved_root.rglob("*.parquet") if scan_recursive else resolved_root.glob("*.parquet"))
        items = [
            {
                "name": item.name,
                "relative_path": item.relative_to(resolved_root).as_posix(),
                "absolute_path": str(item.resolve()),
            }
            for item in files
        ]
        return {"root_path": str(resolved_root), "items": items, "recursive": scan_recursive}

    def list_directories(self, base_path: str, current_path: Optional[str] = None) -> dict[str, object]:
        if self._is_hdfs_uri(base_path):
            normalized_base = base_path.rstrip("/")
            normalized_current = (current_path or normalized_base).rstrip("/")
            if not normalized_current.startswith(normalized_base):
                raise QueryValidationError("Folder path must stay inside configured HDFS base path.")
            items = self._list_hdfs_directories(normalized_current)
            return {
                "base_path": normalized_base,
                "current_path": normalized_current,
                "items": items,
            }

        resolved_base = Path(base_path).expanduser().resolve()
        resolved_current = Path(current_path).expanduser().resolve() if current_path else resolved_base
        if not resolved_base.exists() or not resolved_base.is_dir():
            raise QueryValidationError("Base path must be an existing directory.")
        if resolved_base not in resolved_current.parents and resolved_current != resolved_base:
            raise QueryValidationError("Folder path must stay inside configured base path.")
        items = [
            {
                "name": item.name,
                "relative_path": item.relative_to(resolved_base).as_posix(),
                "absolute_path": str(item.resolve()),
            }
            for item in sorted(resolved_current.iterdir())
            if item.is_dir()
        ]
        return {
            "base_path": str(resolved_base),
            "current_path": str(resolved_current),
            "items": items,
        }

    def resolve_root(self, root_path: Optional[str] = None) -> Any:
        if root_path and self._is_hdfs_uri(root_path):
            return root_path.rstrip("/")
        if root_path:
            candidate = Path(root_path).expanduser().resolve()
        elif isinstance(self.parquet_root, str) and self._is_hdfs_uri(self.parquet_root):
            return self.parquet_root.rstrip("/")
        else:
            candidate = Path(self.parquet_root).expanduser().resolve()
        if candidate.exists() and not candidate.is_dir():
            raise QueryValidationError("Root path must be directory.")
        return candidate

    def resolve_path(
        self,
        selected_file: Optional[str],
        root_path: Optional[str] = None,
        recursive: Optional[bool] = None,
    ) -> Any:
        scan_recursive = self._resolve_recursive(recursive)
        if root_path and self._is_hdfs_uri(root_path):
            root_uri = str(self.resolve_root(root_path))
            if not selected_file or selected_file == ALL_FILES_TOKEN:
                items = self._list_hdfs_files(root_uri, scan_recursive)
                if items:
                    return {"kind": "hdfs-folder", "root_uri": root_uri, "recursive": scan_recursive}
                raise FileNotFoundError(f"No parquet files found under {root_uri}.")
            if not str(selected_file).lower().endswith(".parquet"):
                raise QueryValidationError("Only .parquet files allowed.")
            return self._join_hdfs_path(root_uri, selected_file)

        base_root = self.resolve_root(root_path)
        if not selected_file or selected_file == ALL_FILES_TOKEN:
            files = sorted(base_root.rglob("*.parquet") if scan_recursive else base_root.glob("*.parquet"))
            if files:
                return {"kind": "local-folder", "root": str(base_root), "recursive": scan_recursive}
            raise FileNotFoundError(f"No parquet files found under {base_root}.")

        candidate = (base_root / selected_file).resolve()
        if base_root not in candidate.parents and candidate != base_root:
            raise QueryValidationError("Selected file must stay inside chosen root path.")
        if candidate.suffix.lower() != ".parquet":
            raise QueryValidationError("Only .parquet files allowed.")
        return self.ensure_ready(candidate)

    def describe_schema(
        self,
        selected_file: Optional[str] = None,
        root_path: Optional[str] = None,
        recursive: Optional[bool] = None,
    ) -> list[dict[str, object]]:
        parquet_path = self.resolve_path(selected_file, root_path, recursive)
        with duckdb.connect(database=":memory:") as conn:
            self._register_table(conn, parquet_path)
            rows = conn.execute("DESCRIBE current_parquet").fetchall()
        return [
            {"column": row[0], "type": row[1], "nullable": row[2], "key": row[3], "default": row[4]}
            for row in rows
        ]

    def preview(
        self,
        selected_file: Optional[str] = None,
        root_path: Optional[str] = None,
        recursive: Optional[bool] = None,
        page: int = 1,
        page_size: Optional[int] = None,
    ) -> QueryResult:
        return self.run_query(
            f"SELECT * FROM current_parquet LIMIT {self.max_preview_rows}",
            selected_file=selected_file,
            root_path=root_path,
            recursive=recursive,
            page=page,
            page_size=page_size or min(self.default_page_size, self.max_preview_rows),
        )

    def run_query(
        self,
        raw_query: str,
        selected_file: Optional[str] = None,
        root_path: Optional[str] = None,
        recursive: Optional[bool] = None,
        page: int = 1,
        page_size: Optional[int] = None,
    ) -> QueryResult:
        parquet_path = self.resolve_path(selected_file, root_path, recursive)
        query = self._normalize_query(raw_query)
        normalized_page = max(page, 1)
        normalized_page_size = min(max(page_size or self.default_page_size, 1), self.max_page_size)
        offset = (normalized_page - 1) * normalized_page_size
        started = time.perf_counter()
        with duckdb.connect(database=":memory:") as conn:
            self._register_table(conn, parquet_path)
            count_cursor = conn.execute(f"SELECT COUNT(*) FROM ({query}) AS current_result")
            total_rows = int(count_cursor.fetchone()[0])
            cursor = conn.execute(
                f"SELECT * FROM ({query}) AS current_result LIMIT {normalized_page_size} OFFSET {offset}"
            )
            rows = cursor.fetchall()
            columns = [item[0] for item in cursor.description] if cursor.description else []
        elapsed_ms = round((time.perf_counter() - started) * 1000, 2)
        total_pages = max((total_rows + normalized_page_size - 1) // normalized_page_size, 1)
        return QueryResult(
            columns=columns,
            rows=[list(row) for row in rows],
            row_count=len(rows),
            total_rows=total_rows,
            page=normalized_page,
            page_size=normalized_page_size,
            total_pages=total_pages,
            elapsed_ms=elapsed_ms,
            query=query,
        )

    def upload_parquet(
        self,
        destination_root: Optional[str],
        filename: str,
        payload: bytes,
    ) -> dict[str, str]:
        root = self.resolve_root(destination_root)
        root.mkdir(parents=True, exist_ok=True)
        safe_name = Path(filename).name
        if not safe_name.lower().endswith(".parquet"):
            raise QueryValidationError("Only .parquet uploads are allowed.")
        if len(payload) > self.max_upload_mb * 1024 * 1024:
            raise QueryValidationError(f"Upload exceeds {self.max_upload_mb} MB limit.")

        temp_path = root / f".upload-{safe_name}"
        final_path = root / safe_name
        if final_path.exists():
            raise QueryValidationError("A file with that name already exists in the selected root path.")
        temp_path.write_bytes(payload)
        try:
            safe_temp_path = str(temp_path).replace("'", "''")
            with duckdb.connect(database=":memory:") as conn:
                conn.execute(f"SELECT * FROM read_parquet('{safe_temp_path}') LIMIT 1").fetchall()
        except Exception as exc:  # noqa: BLE001
            temp_path.unlink(missing_ok=True)
            raise QueryValidationError("Uploaded file is not a valid parquet file.") from exc

        move(str(temp_path), str(final_path))
        return {"filename": safe_name, "stored_path": str(final_path.resolve())}

    def _register_table(self, conn: duckdb.DuckDBPyConnection, parquet_path: Any) -> None:
        if isinstance(parquet_path, dict):
            dataset_kind = parquet_path.get("kind")
            if dataset_kind == "local-folder":
                self._register_local_folder(
                    conn,
                    Path(str(parquet_path["root"])),
                    bool(parquet_path.get("recursive", False)),
                )
                return
            if dataset_kind == "hdfs-folder":
                self._register_hdfs_folder(
                    conn,
                    str(parquet_path["root_uri"]),
                    bool(parquet_path.get("recursive", False)),
                )
                return

        parquet_reference = str(parquet_path)
        if self._is_hdfs_uri(parquet_reference):
            parquet_reference = self._copy_hdfs_parquet_to_local(parquet_reference)

        safe_path = str(parquet_path).replace("'", "''")
        if self._is_hdfs_uri(str(parquet_path)):
            safe_path = str(parquet_reference).replace("'", "''")
        conn.execute(
            f"CREATE OR REPLACE VIEW current_parquet AS SELECT * FROM read_parquet('{safe_path}')"
        )

    def _register_local_folder(
        self,
        conn: duckdb.DuckDBPyConnection,
        base_root: Path,
        recursive: bool,
    ) -> None:
        files = sorted(base_root.rglob("*.parquet") if recursive else base_root.glob("*.parquet"))
        if not files:
            raise FileNotFoundError(f"No parquet files found under {base_root}.")
        parquet_list = ", ".join(self._quote_sql_string(str(path)) for path in files)
        conn.execute(
            f"CREATE OR REPLACE VIEW current_parquet AS SELECT * FROM read_parquet([{parquet_list}])"
        )

    def _register_hdfs_folder(
        self,
        conn: duckdb.DuckDBPyConnection,
        root_uri: str,
        recursive: bool,
    ) -> None:
        local_files = self._copy_hdfs_folder_to_local(root_uri, recursive)
        parquet_list = ", ".join(self._quote_sql_string(path) for path in local_files)
        conn.execute(
            f"CREATE OR REPLACE VIEW current_parquet AS SELECT * FROM read_parquet([{parquet_list}])"
        )

    def _normalize_query(self, raw_query: str) -> str:
        query = raw_query.strip().rstrip(";")
        if not query:
            raise QueryValidationError("Query required.")
        if ";" in query:
            raise QueryValidationError("Only single statement queries allowed.")

        lowered = query.lower()
        if not lowered.startswith(ALLOWED_PREFIXES):
            raise QueryValidationError(
                "Only read queries allowed: SELECT, WITH, SHOW, DESCRIBE, EXPLAIN."
            )
        for token in BLOCKED_TOKENS:
            if f"{token} " in lowered or lowered.endswith(token):
                raise QueryValidationError(f"Blocked keyword detected: {token.upper()}.")
        return query

    def _resolve_recursive(self, recursive: Optional[bool]) -> bool:
        return self.allow_recursive_scan if recursive is None else recursive

    def _is_hdfs_uri(self, value: Optional[str]) -> bool:
        return bool(value and str(value).startswith("hdfs://"))

    def _list_hdfs_files(self, root_uri: str, recursive: bool) -> list[dict[str, str]]:
        self.ensure_hdfs_dependency()
        command = ["hdfs", "dfs", "-ls"]
        if recursive:
            command.append("-R")
        command.append(root_uri)
        try:
            result = subprocess.run(
                command,
                check=True,
                capture_output=True,
                text=True,
            )
        except subprocess.CalledProcessError as exc:
            raise QueryValidationError(exc.stderr.strip() or "Unable to list HDFS path.") from exc

        items = []
        base_path = root_uri.rstrip("/")
        for line in result.stdout.splitlines():
            line = line.strip()
            if not line or line.startswith("Found "):
                continue
            parts = line.split()
            if len(parts) < 8 or not parts[0].startswith("-"):
                continue
            file_path = parts[-1]
            if not file_path.lower().endswith(".parquet"):
                continue
            relative_path = file_path[len(base_path) + 1 :] if file_path.startswith(f"{base_path}/") else Path(file_path).name
            items.append(
                {
                    "name": Path(file_path).name,
                    "relative_path": relative_path,
                    "absolute_path": self._join_hdfs_path(root_uri, relative_path),
                }
            )
        return items

    def _list_hdfs_directories(self, base_uri: str) -> list[dict[str, str]]:
        self.ensure_hdfs_dependency()
        try:
            result = subprocess.run(
                ["hdfs", "dfs", "-ls", base_uri],
                check=True,
                capture_output=True,
                text=True,
            )
        except subprocess.CalledProcessError as exc:
            raise QueryValidationError(exc.stderr.strip() or "Unable to list HDFS base path.") from exc

        items: list[dict[str, str]] = []
        for line in result.stdout.splitlines():
            line = line.strip()
            if not line or line.startswith("Found "):
                continue
            parts = line.split()
            if len(parts) < 8 or not parts[0].startswith("d"):
                continue
            directory_path = parts[-1]
            relative_path = (
                directory_path[len(base_uri) + 1 :] if directory_path.startswith(f"{base_uri}/") else Path(directory_path).name
            )
            items.append(
                {
                    "name": Path(directory_path).name,
                    "relative_path": relative_path,
                    "absolute_path": directory_path,
                }
            )
        return items

    def _join_hdfs_path(self, root_uri: str, relative_path: str) -> str:
        return f"{root_uri.rstrip('/')}/{relative_path.lstrip('/')}"

    def _copy_hdfs_parquet_to_local(self, parquet_uri: str) -> str:
        self.ensure_hdfs_dependency()
        cache_root = Path(tempfile.gettempdir()) / "parquet_viewer_hdfs_cache"
        cache_root.mkdir(parents=True, exist_ok=True)
        cache_path = cache_root / f"{hashlib.sha256(parquet_uri.encode('utf-8')).hexdigest()}.parquet"
        try:
            subprocess.run(
                ["hdfs", "dfs", "-copyToLocal", "-f", parquet_uri, str(cache_path)],
                check=True,
                capture_output=True,
                text=True,
            )
        except subprocess.CalledProcessError as exc:
            raise QueryValidationError(exc.stderr.strip() or "Unable to read parquet file from HDFS.") from exc
        return str(cache_path)

    def _copy_hdfs_folder_to_local(self, root_uri: str, recursive: bool) -> list[str]:
        items = self._list_hdfs_files(root_uri, recursive)
        if not items:
            raise FileNotFoundError(f"No parquet files found under {root_uri}.")

        cache_root = Path(tempfile.gettempdir()) / "parquet_viewer_hdfs_cache" / hashlib.sha256(
            f"{root_uri}|{recursive}".encode("utf-8")
        ).hexdigest()
        cache_root.mkdir(parents=True, exist_ok=True)

        local_files: list[str] = []
        for item in items:
            relative_path = str(item["relative_path"])
            file_uri = str(item["absolute_path"])
            local_name = f"{hashlib.sha256(relative_path.encode('utf-8')).hexdigest()}.parquet"
            local_path = cache_root / local_name
            try:
                subprocess.run(
                    ["hdfs", "dfs", "-copyToLocal", "-f", file_uri, str(local_path)],
                    check=True,
                    capture_output=True,
                    text=True,
                )
            except subprocess.CalledProcessError as exc:
                raise QueryValidationError(
                    exc.stderr.strip() or f"Unable to read parquet file from HDFS: {relative_path}."
                ) from exc
            local_files.append(str(local_path))
        return local_files

    def _quote_sql_string(self, value: str) -> str:
        return "'" + value.replace("'", "''") + "'"
