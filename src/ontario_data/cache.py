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
    """Check for semicolons outside of single-quoted string literals."""
    in_string = False
    for i, char in enumerate(sql):
        if char == "'" and (i == 0 or sql[i - 1] != "\\"):
            in_string = not in_string
        elif char == ";" and not in_string:
            return True
    return False


def _validate_sql(sql: str) -> None:
    """Validate that SQL is read-only and safe.

    Raises InvalidQueryError for mutations or injection attempts.
    """
    # Strip leading whitespace and comments
    cleaned = re.sub(r"(/\*.*?\*/|--[^\n]*\n?)", "", sql, flags=re.DOTALL).strip()

    # Reject semicolons outside string literals (defense-in-depth against injection)
    if _has_semicolons_outside_strings(sql):
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
        """Open a raw DuckDB connection without loading extensions."""
        conn = duckdb.connect(self.db_path)
        try:
            yield conn
        finally:
            conn.close()

    @contextmanager
    def _connect(self):
        """Open a DuckDB connection with extensions loaded."""
        conn = duckdb.connect(self.db_path)
        try:
            for ext in self._extensions:
                conn.execute(f"LOAD {ext}")
            yield conn
        finally:
            conn.close()

    def _with_retry(self, fn, max_attempts=3):
        """Retry fn(conn) on DuckDB IO errors (lock contention)."""
        for attempt in range(max_attempts):
            try:
                with self._connect() as conn:
                    return fn(conn)
            except duckdb.IOException:
                if attempt == max_attempts - 1:
                    raise
                time.sleep(0.1 * (attempt + 1))

    def initialize(self):
        """Create metadata tables and install extensions."""
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
        """Whether the DuckDB spatial extension is loaded."""
        return self._has_spatial

    def store_resource(
        self,
        resource_id: str,
        dataset_id: str,
        table_name: str,
        df: pd.DataFrame,
        source_url: str,
    ):
        """Store a pandas DataFrame as a DuckDB table."""
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

            # Record metadata
            now = datetime.now(timezone.utc)
            size = df.memory_usage(deep=True).sum()
            conn.execute(
                """INSERT INTO _cache_metadata
                   (resource_id, dataset_id, table_name, downloaded_at, row_count, size_bytes, source_url)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                [resource_id, dataset_id, table_name, now, len(df), int(size), source_url],
            )

        self._with_retry(_do)

    def is_cached(self, resource_id: str) -> bool:
        """Check if a resource is in the cache."""
        with self._connect() as conn:
            result = conn.execute(
                "SELECT 1 FROM _cache_metadata WHERE resource_id = ?", [resource_id]
            ).fetchone()
            return result is not None

    def get_table_name(self, resource_id: str) -> str | None:
        """Get the DuckDB table name for a cached resource."""
        with self._connect() as conn:
            result = conn.execute(
                "SELECT table_name FROM _cache_metadata WHERE resource_id = ?", [resource_id]
            ).fetchone()
            return result[0] if result else None

    def list_cached(self) -> list[dict[str, Any]]:
        """List all cached resources."""
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
        """Remove a resource from the cache."""
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
        """Remove all cached resources."""
        def _do(conn):
            rows = conn.execute(
                "SELECT table_name FROM _cache_metadata"
            ).fetchall()
            for row in rows:
                conn.execute(f'DROP TABLE IF EXISTS "{row[0]}"')
            conn.execute("DELETE FROM _cache_metadata")

        self._with_retry(_do)

    def get_stats(self) -> dict[str, Any]:
        """Get cache statistics."""
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
        """Run a validated read-only SQL query against the cache."""
        _validate_sql(sql)
        with self._connect() as conn:
            result = conn.execute(sql)
            columns = [desc[0] for desc in result.description]
            rows = result.fetchall()
            return [dict(zip(columns, row)) for row in rows]

    def query_df(self, sql: str) -> pd.DataFrame:
        """Run a validated read-only SQL query and return a DataFrame."""
        _validate_sql(sql)
        with self._connect() as conn:
            return conn.execute(sql).fetchdf()

    def update_expires_at(self, resource_id: str, expires_at):
        """Set the expires_at timestamp for a cached resource."""
        def _do(conn):
            conn.execute(
                "UPDATE _cache_metadata SET expires_at = ? WHERE resource_id = ?",
                [expires_at, resource_id],
            )

        self._with_retry(_do)

    def store_dataset_metadata(self, dataset_id: str, metadata: dict[str, Any]):
        """Cache dataset metadata."""
        def _do(conn):
            now = datetime.now(timezone.utc)
            conn.execute(
                """INSERT OR REPLACE INTO _dataset_metadata (dataset_id, metadata, cached_at)
                   VALUES (?, ?, ?)""",
                [dataset_id, json.dumps(metadata), now],
            )

        self._with_retry(_do)

    def get_dataset_metadata(self, dataset_id: str) -> dict[str, Any] | None:
        """Get cached dataset metadata."""
        with self._connect() as conn:
            result = conn.execute(
                "SELECT metadata FROM _dataset_metadata WHERE dataset_id = ?", [dataset_id]
            ).fetchone()
            if result:
                return json.loads(result[0])
            return None

    def execute_sql(self, sql: str, params=None) -> list[tuple]:
        """Execute SQL and return raw tuples."""
        with self._connect() as conn:
            result = conn.execute(sql, params or [])
            return result.fetchall()

    def execute_sql_dict(self, sql: str, params=None) -> list[dict[str, Any]]:
        """Execute SQL and return list of dicts with column names."""
        with self._connect() as conn:
            result = conn.execute(sql, params or [])
            columns = [desc[0] for desc in result.description]
            rows = result.fetchall()
            return [dict(zip(columns, row)) for row in rows]

    def get_resource_meta(self, resource_id: str) -> dict[str, Any] | None:
        """Get full cache metadata for a resource."""
        with self._connect() as conn:
            row = conn.execute(
                "SELECT resource_id, dataset_id, table_name, downloaded_at, "
                "row_count, size_bytes, source_url, expires_at "
                "FROM _cache_metadata WHERE resource_id = ?",
                [resource_id],
            ).fetchone()
            if row is None:
                return None
            cols = [
                "resource_id", "dataset_id", "table_name", "downloaded_at",
                "row_count", "size_bytes", "source_url", "expires_at",
            ]
            return dict(zip(cols, row))
