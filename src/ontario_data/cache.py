from __future__ import annotations

import json
import logging
import os
import re
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any

import duckdb
import pandas as pd

logger = logging.getLogger("ontario_data.cache")

# SQL statements allowed for user queries
_ALLOWED_PREFIXES = ("select", "with", "explain", "describe", "show", "pragma", "summarize")


class InvalidQueryError(Exception):
    """Raised for invalid or unsafe SQL queries."""
    pass


def _has_semicolons_outside_strings(sql: str) -> bool:
    """Defense against statement-stacking injection (e.g. 'SELECT 1; DROP TABLE').

    Uses SQL-standard '' (doubled single-quote) escaping, NOT backslash escaping.
    """
    in_string = False
    i = 0
    while i < len(sql):
        char = sql[i]
        if char == "'" and not in_string:
            in_string = True
        elif char == "'" and in_string:
            # SQL-standard escape: '' means a literal single quote
            if i + 1 < len(sql) and sql[i + 1] == "'":
                i += 1  # skip the second quote
            else:
                in_string = False
        elif char == ";" and not in_string:
            return True
        i += 1
    return False


def _validate_sql(sql: str) -> None:
    """Validate that SQL is read-only and safe.

    Raises InvalidQueryError for mutations or injection attempts.
    """
    # Strip leading whitespace and comments
    cleaned = re.sub(r"(/\*.*?\*/|--[^\n]*\n?)", "", sql, flags=re.DOTALL).strip()

    # Reject semicolons outside string literals (defense-in-depth against injection).
    # Check comment-stripped SQL so a quote inside a comment (e.g. --')
    # can't open a fake "string" that hides a real semicolon.
    if _has_semicolons_outside_strings(cleaned):
        raise InvalidQueryError(
            "SQL queries must not contain semicolons outside string literals. "
            "Send one statement at a time."
        )

    # Check statement starts with allowed prefix
    first_word = cleaned.split()[0].lower() if cleaned.split() else ""
    if first_word not in _ALLOWED_PREFIXES:
        raise InvalidQueryError(
            f"Only read-only queries are allowed. "
            f"Got '{first_word}...'. Use SELECT, WITH, EXPLAIN, DESCRIBE, SHOW, or SUMMARIZE."
        )


class CacheManager:
    """DuckDB-backed cache and analytics engine for Ontario open data.

    Opens a short-lived connection per operation so multiple MCP server
    processes can share the same database file without lock contention.
    """

    def __init__(self, db_path: str | None = None):
        if db_path is None:
            cache_dir = os.path.expanduser(
                os.environ.get("ONTARIO_DATA_CACHE_DIR", "~/.cache/ontario-data")
            )
            os.makedirs(cache_dir, exist_ok=True)
            db_path = os.path.join(cache_dir, "ontario_data.duckdb")
        self.db_path = db_path
        self._extensions: list[str] = []
        self._has_spatial = False

    @contextmanager
    def _connect_raw(self):
        conn = duckdb.connect(self.db_path)
        try:
            yield conn
        finally:
            conn.close()

    @contextmanager
    def _connect(self):
        conn = duckdb.connect(self.db_path)
        try:
            for ext in self._extensions:
                conn.execute(f"LOAD {ext}")
            yield conn
        finally:
            conn.close()

    def _with_retry(self, fn, max_attempts=3):
        """Retry with linear backoff (100ms, 200ms, 300ms) when another
        process holds the DuckDB file lock."""
        for attempt in range(max_attempts):
            try:
                with self._connect() as conn:
                    return fn(conn)
            except duckdb.IOException:
                if attempt == max_attempts - 1:
                    raise
                time.sleep(0.1 * (attempt + 1))

    def initialize(self):
        """Set up schema and extensions. Must be called once before any
        other operations — typically during server lifespan startup."""
        with self._connect_raw() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS _cache_metadata (
                    resource_id VARCHAR PRIMARY KEY,
                    dataset_id VARCHAR,
                    table_name VARCHAR,
                    downloaded_at TIMESTAMP,
                    row_count INTEGER,
                    size_bytes BIGINT,
                    source_url VARCHAR,
                    expires_at TIMESTAMP
                )
            """)
            # Migration: add type_warnings column if missing
            try:
                conn.execute("ALTER TABLE _cache_metadata ADD COLUMN type_warnings JSON")
            except Exception:
                pass  # column already exists
            conn.execute("""
                CREATE TABLE IF NOT EXISTS _dataset_metadata (
                    dataset_id VARCHAR PRIMARY KEY,
                    metadata JSON,
                    cached_at TIMESTAMP
                )
            """)
            # Install and load extensions
            for ext in ["httpfs", "json"]:
                try:
                    conn.execute(f"INSTALL {ext}")
                    conn.execute(f"LOAD {ext}")
                    self._extensions.append(ext)
                except Exception:
                    try:
                        conn.execute(f"LOAD {ext}")
                        self._extensions.append(ext)
                    except Exception:
                        pass

            # Spatial extension — track availability
            try:
                conn.execute("INSTALL spatial")
                conn.execute("LOAD spatial")
                self._extensions.append("spatial")
                self._has_spatial = True
            except Exception:
                try:
                    conn.execute("LOAD spatial")
                    self._extensions.append("spatial")
                    self._has_spatial = True
                except Exception:
                    self._has_spatial = False
                    logger.info("DuckDB spatial extension not available")

    @property
    def has_spatial_extension(self) -> bool:
        return self._has_spatial

    @staticmethod
    def _detect_numeric_varchars(conn, table_name: str) -> list[dict]:
        """Detect VARCHAR columns whose values look numeric.

        Returns a list of dicts with 'name' and 'has_commas' keys.
        """
        columns = conn.execute(f'DESCRIBE "{table_name}"').fetchall()
        plain_re = re.compile(r"^-?\d+\.?\d*$")
        comma_re = re.compile(r"^-?\d{1,3}(,\d{3})+(\.\d+)?$")
        suspects: list[dict] = []
        for col in columns:
            col_name, col_type = col[0], str(col[1])
            if "VARCHAR" not in col_type.upper():
                continue
            sample = conn.execute(
                f'SELECT DISTINCT "{col_name}" FROM "{table_name}" '
                f'WHERE "{col_name}" IS NOT NULL LIMIT 100'
            ).fetchall()
            values = [str(r[0]).strip() for r in sample if r[0] is not None and str(r[0]).strip()]
            if not values:
                continue
            plain_count = sum(1 for v in values if plain_re.match(v))
            comma_count = sum(1 for v in values if comma_re.match(v))
            numeric_count = plain_count + comma_count
            if numeric_count / len(values) > 0.8:
                suspects.append({"name": col_name, "has_commas": comma_count > 0})
        return suspects

    def store_resource(
        self,
        resource_id: str,
        dataset_id: str,
        table_name: str,
        df: pd.DataFrame,
        source_url: str,
    ):
        """Upsert: drops the previous table for this resource_id (if any)
        before creating the new one, so re-downloads are safe."""
        def _do(conn):
            # Drop existing table if re-caching
            old = conn.execute(
                "SELECT table_name FROM _cache_metadata WHERE resource_id = ?",
                [resource_id],
            ).fetchone()
            if old:
                conn.execute(f'DROP TABLE IF EXISTS "{old[0]}"')
                conn.execute(
                    "DELETE FROM _cache_metadata WHERE resource_id = ?", [resource_id]
                )

            # Create table from DataFrame
            conn.execute(f'CREATE TABLE "{table_name}" AS SELECT * FROM df')

            # Detect VARCHAR columns that look numeric and auto-cast to DOUBLE
            numeric_varchars = self._detect_numeric_varchars(conn, table_name)
            for col_info in numeric_varchars:
                c = col_info["name"].replace('"', '""')
                if col_info["has_commas"]:
                    expr = f'TRY_CAST(REPLACE("{c}", \',\', \'\') AS DOUBLE)'
                else:
                    expr = f'TRY_CAST("{c}" AS DOUBLE)'
                try:
                    conn.execute(
                        f'ALTER TABLE "{table_name}" ALTER "{c}" '
                        f'TYPE DOUBLE USING {expr}'
                    )
                except Exception:
                    logger.debug("Failed to auto-cast column %s in %s", c, table_name, exc_info=True)

            # Record metadata
            now = datetime.now(timezone.utc)
            size_row = conn.execute(
                "SELECT estimated_size FROM duckdb_tables() WHERE table_name = ?",
                [table_name],
            ).fetchone()
            size = size_row[0] if size_row else 0
            conn.execute(
                """INSERT INTO _cache_metadata
                   (resource_id, dataset_id, table_name, downloaded_at, row_count, size_bytes, source_url, type_warnings)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                [resource_id, dataset_id, table_name, now, len(df), int(size), source_url, None],
            )

        self._with_retry(_do)

    def is_cached(self, resource_id: str) -> bool:
        with self._connect() as conn:
            result = conn.execute(
                "SELECT 1 FROM _cache_metadata WHERE resource_id = ?", [resource_id]
            ).fetchone()
            return result is not None

    def get_table_name(self, resource_id: str) -> str | None:
        with self._connect() as conn:
            result = conn.execute(
                "SELECT table_name FROM _cache_metadata WHERE resource_id = ?", [resource_id]
            ).fetchone()
            return result[0] if result else None

    def list_cached(self) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT resource_id, dataset_id, table_name, downloaded_at, row_count, size_bytes, source_url "
                "FROM _cache_metadata ORDER BY downloaded_at DESC"
            ).fetchall()
            return [
                {
                    "resource_id": r[0],
                    "dataset_id": r[1],
                    "table_name": r[2],
                    "downloaded_at": str(r[3]),
                    "row_count": r[4],
                    "size_bytes": r[5],
                    "source_url": r[6],
                }
                for r in rows
            ]

    def remove_resource(self, resource_id: str):
        def _do(conn):
            result = conn.execute(
                "SELECT table_name FROM _cache_metadata WHERE resource_id = ?",
                [resource_id],
            ).fetchone()
            if result:
                conn.execute(f'DROP TABLE IF EXISTS "{result[0]}"')
            conn.execute(
                "DELETE FROM _cache_metadata WHERE resource_id = ?", [resource_id]
            )

        self._with_retry(_do)

    def remove_all(self):
        def _do(conn):
            rows = conn.execute(
                "SELECT table_name FROM _cache_metadata"
            ).fetchall()
            for row in rows:
                conn.execute(f'DROP TABLE IF EXISTS "{row[0]}"')
            conn.execute("DELETE FROM _cache_metadata")

        self._with_retry(_do)

    def get_stats(self) -> dict[str, Any]:
        with self._connect() as conn:
            result = conn.execute(
                "SELECT count(*), coalesce(sum(row_count), 0), coalesce(sum(size_bytes), 0) "
                "FROM _cache_metadata"
            ).fetchone()
            return {
                "table_count": result[0],
                "total_rows": result[1],
                "total_size_bytes": result[2],
                "db_path": self.db_path,
            }

    def query(self, sql: str) -> list[dict[str, Any]]:
        """Run a validated read-only SQL query against the cache.

        Validates that *sql* is a safe, read-only statement before executing.
        Use this for user- or LLM-supplied SQL (e.g. the query_cached tool).
        For programmatically-built SQL from trusted internal code, use
        execute_sql() or execute_sql_dict() instead.
        """
        rows, _ = self.query_with_meta(sql)
        return rows

    def query_with_meta(
        self, sql: str, max_rows: int | None = None
    ) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
        """Like query() but also returns column metadata.

        Returns (rows, fields) where fields is a list of
        {"name": col_name, "type": duckdb_type_name} dicts.

        If max_rows is set, fetches at most max_rows + 1 rows (so the
        caller can detect truncation) instead of materializing the full
        result set.
        """
        _validate_sql(sql)
        with self._connect() as conn:
            result = conn.execute(sql)
            description = result.description
            columns = [desc[0] for desc in description]
            type_names = [str(desc[1]) for desc in description]
            if max_rows is not None:
                raw_rows = result.fetchmany(max_rows + 1)
            else:
                raw_rows = result.fetchall()
            rows = [dict(zip(columns, row)) for row in raw_rows]
            fields = [{"name": col, "type": typ} for col, typ in zip(columns, type_names)]
            return rows, fields

    def update_expires_at(self, resource_id: str, expires_at):
        def _do(conn):
            conn.execute(
                "UPDATE _cache_metadata SET expires_at = ? WHERE resource_id = ?",
                [expires_at, resource_id],
            )

        self._with_retry(_do)

    def store_dataset_metadata(self, dataset_id: str, metadata: dict[str, Any]):
        def _do(conn):
            now = datetime.now(timezone.utc)
            conn.execute(
                """INSERT OR REPLACE INTO _dataset_metadata (dataset_id, metadata, cached_at)
                   VALUES (?, ?, ?)""",
                [dataset_id, json.dumps(metadata), now],
            )

        self._with_retry(_do)

    def get_dataset_metadata(self, dataset_id: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            result = conn.execute(
                "SELECT metadata FROM _dataset_metadata WHERE dataset_id = ?", [dataset_id]
            ).fetchone()
            if result:
                return json.loads(result[0])
            return None

    def execute_sql(self, sql: str, params=None) -> list[tuple]:
        """Execute SQL without validation and return raw tuples.

        For internal/programmatic use only (e.g. quality checks, spatial
        queries). Use query() for user-supplied SQL, which validates
        read-only safety first.
        """
        with self._connect() as conn:
            result = conn.execute(sql, params or [])
            return result.fetchall()

    def execute_sql_dict(self, sql: str, params=None) -> list[dict[str, Any]]:
        """Execute SQL without validation and return dicts with column names.

        For internal/programmatic use only. See execute_sql() for details.
        """
        with self._connect() as conn:
            result = conn.execute(sql, params or [])
            columns = [desc[0] for desc in result.description]
            rows = result.fetchall()
            return [dict(zip(columns, row)) for row in rows]

    def get_tables_metadata(self, table_names: list[str]) -> list[dict[str, Any]]:
        """Look up cache metadata by table name(s)."""
        if not table_names:
            return []
        with self._connect() as conn:
            placeholders = ", ".join("?" for _ in table_names)
            rows = conn.execute(
                f"SELECT resource_id, dataset_id, table_name, downloaded_at, "
                f"row_count, expires_at "
                f"FROM _cache_metadata WHERE table_name IN ({placeholders})",
                table_names,
            ).fetchall()
            cols = ["resource_id", "dataset_id", "table_name", "downloaded_at",
                    "row_count", "expires_at"]
            return [dict(zip(cols, row)) for row in rows]

    def get_resource_meta(self, resource_id: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT resource_id, dataset_id, table_name, downloaded_at, "
                "row_count, size_bytes, source_url, expires_at, type_warnings "
                "FROM _cache_metadata WHERE resource_id = ?",
                [resource_id],
            ).fetchone()
            if row is None:
                return None
            cols = [
                "resource_id", "dataset_id", "table_name", "downloaded_at",
                "row_count", "size_bytes", "source_url", "expires_at", "type_warnings",
            ]
            meta = dict(zip(cols, row))
            # Parse JSON type_warnings
            if meta["type_warnings"]:
                meta["type_warnings"] = json.loads(meta["type_warnings"])
            return meta
