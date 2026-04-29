from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path
from shutil import move
from typing import Any, Optional

import duckdb


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
            import pyarrow.fs  # noqa: F401
            import pyarrow.parquet  # noqa: F401
        except ImportError as exc:  # pragma: no cover - dependency driven
            raise QueryValidationError(
                "HDFS support requires pyarrow. Install requirements and ensure HDFS client libraries are configured."
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

    def resolve_root(self, root_path: Optional[str] = None) -> Any:
        if root_path and self._is_hdfs_uri(root_path):
            return root_path.rstrip("/")
        candidate = Path(root_path).expanduser().resolve() if root_path else self.parquet_root.resolve()
        if candidate.exists() and not candidate.is_dir():
            raise QueryValidationError("Root path must be directory.")
        return candidate

    def resolve_path(
        self,
        selected_file: Optional[str],
        root_path: Optional[str] = None,
        recursive: Optional[bool] = None,
    ) -> Any:
        if root_path and self._is_hdfs_uri(root_path):
            root_uri = str(self.resolve_root(root_path))
            if not selected_file:
                items = self._list_hdfs_files(root_uri, self._resolve_recursive(recursive))
                if items:
                    return self._join_hdfs_path(root_uri, items[0]["relative_path"])
                raise FileNotFoundError(f"No parquet files found under {root_uri}.")
            if not str(selected_file).lower().endswith(".parquet"):
                raise QueryValidationError("Only .parquet files allowed.")
            return self._join_hdfs_path(root_uri, selected_file)

        base_root = self.resolve_root(root_path)
        if not selected_file:
            default_path = self.parquet_path
            if default_path.exists() and default_path.parent == base_root:
                return self.ensure_ready(default_path)
            files = sorted(
                base_root.rglob("*.parquet") if self._resolve_recursive(recursive) else base_root.glob("*.parquet")
            )
            if files:
                return self.ensure_ready(files[0])
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
        parquet_reference = str(parquet_path)
        if self._is_hdfs_uri(parquet_reference):
            arrow_table = self._read_hdfs_parquet(parquet_reference)
            conn.register("current_parquet_arrow", arrow_table)
            conn.execute("CREATE OR REPLACE VIEW current_parquet AS SELECT * FROM current_parquet_arrow")
            return

        safe_path = str(parquet_path).replace("'", "''")
        conn.execute(
            f"CREATE OR REPLACE VIEW current_parquet AS SELECT * FROM read_parquet('{safe_path}')"
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
        import pyarrow.fs as pafs

        filesystem, path = pafs.FileSystem.from_uri(root_uri)
        selector = pafs.FileSelector(path, recursive=recursive)
        file_infos = filesystem.get_file_info(selector)
        base_path = path.rstrip("/")
        items = []
        for item in file_infos:
            if not item.is_file or not item.path.lower().endswith(".parquet"):
                continue
            relative_path = item.path[len(base_path) + 1 :] if base_path else item.path
            items.append(
                {
                    "name": Path(item.path).name,
                    "relative_path": relative_path,
                    "absolute_path": self._join_hdfs_path(root_uri, relative_path),
                }
            )
        return items

    def _join_hdfs_path(self, root_uri: str, relative_path: str) -> str:
        return f"{root_uri.rstrip('/')}/{relative_path.lstrip('/')}"

    def _read_hdfs_parquet(self, parquet_uri: str) -> Any:
        self.ensure_hdfs_dependency()
        import pyarrow.fs as pafs
        import pyarrow.parquet as pq

        filesystem, path = pafs.FileSystem.from_uri(parquet_uri)
        return pq.read_table(path, filesystem=filesystem)
