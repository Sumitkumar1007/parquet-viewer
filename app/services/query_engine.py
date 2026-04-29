from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path

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
    elapsed_ms: float
    query: str


class QueryEngine:
    def __init__(self, parquet_path: Path, parquet_root: Path, max_preview_rows: int) -> None:
        self.parquet_path = parquet_path
        self.parquet_root = parquet_root
        self.max_preview_rows = max_preview_rows

    def ensure_ready(self, parquet_path: Path | None = None) -> Path:
        target_path = parquet_path or self.parquet_path
        if not target_path.exists():
            raise FileNotFoundError(
                f"Parquet file not found: {target_path}. "
                "Set PARQUET_PATH or generate sample data."
            )
        return target_path

    def list_files(self, root_path: str | None = None) -> dict[str, object]:
        resolved_root = self.resolve_root(root_path)
        resolved_root.mkdir(parents=True, exist_ok=True)
        files = sorted(resolved_root.glob("*.parquet"))
        items = [
            {
                "name": item.name,
                "relative_path": item.relative_to(resolved_root).as_posix(),
                "absolute_path": str(item.resolve()),
            }
            for item in files
        ]
        return {"root_path": str(resolved_root), "items": items}

    def resolve_root(self, root_path: str | None = None) -> Path:
        candidate = Path(root_path).expanduser().resolve() if root_path else self.parquet_root.resolve()
        if candidate.exists() and not candidate.is_dir():
            raise QueryValidationError("Root path must be directory.")
        return candidate

    def resolve_path(self, selected_file: str | None, root_path: str | None = None) -> Path:
        base_root = self.resolve_root(root_path)
        if not selected_file:
            default_path = self.parquet_path
            if default_path.exists() and default_path.parent == base_root:
                return self.ensure_ready(default_path)
            files = sorted(base_root.glob("*.parquet"))
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
        selected_file: str | None = None,
        root_path: str | None = None,
    ) -> list[dict[str, object]]:
        parquet_path = self.resolve_path(selected_file, root_path)
        with duckdb.connect(database=":memory:") as conn:
            self._register_table(conn, parquet_path)
            rows = conn.execute("DESCRIBE current_parquet").fetchall()
        return [
            {"column": row[0], "type": row[1], "nullable": row[2], "key": row[3], "default": row[4]}
            for row in rows
        ]

    def preview(self, selected_file: str | None = None, root_path: str | None = None) -> QueryResult:
        return self.run_query(
            f"SELECT * FROM current_parquet LIMIT {self.max_preview_rows}",
            selected_file=selected_file,
            root_path=root_path,
        )

    def run_query(
        self,
        raw_query: str,
        selected_file: str | None = None,
        root_path: str | None = None,
    ) -> QueryResult:
        parquet_path = self.resolve_path(selected_file, root_path)
        query = self._normalize_query(raw_query)
        started = time.perf_counter()
        with duckdb.connect(database=":memory:") as conn:
            self._register_table(conn, parquet_path)
            cursor = conn.execute(query)
            rows = cursor.fetchall()
            columns = [item[0] for item in cursor.description] if cursor.description else []
        elapsed_ms = round((time.perf_counter() - started) * 1000, 2)
        return QueryResult(
            columns=columns,
            rows=[list(row) for row in rows],
            row_count=len(rows),
            elapsed_ms=elapsed_ms,
            query=query,
        )

    def _register_table(self, conn: duckdb.DuckDBPyConnection, parquet_path: Path) -> None:
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
