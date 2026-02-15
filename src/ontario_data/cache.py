from __future__ import annotations

import json
import logging
import os
import re
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


def _validate_sql(sql: str) -> None:
    """Validate that SQL is read-only and safe.

    Raises InvalidQueryError for mutations or injection attempts.
    """
    # Strip leading whitespace and comments
    cleaned = re.sub(r"(/\*.*?\*/|--[^\n]*\n?)", "", sql, flags=re.DOTALL).strip()

    # Reject semicolons anywhere (defense-in-depth against injection)
    if ";" in sql:
        raise InvalidQueryError(
            "SQL queries must not contain semicolons. Send one statement at a time."
        )

    # Check statement starts with allowed prefix
    first_word = cleaned.split()[0].lower() if cleaned.split() else ""
    if first_word not in _ALLOWED_PREFIXES:
        raise InvalidQueryError(
            f"Only read-only queries are allowed. "
            f"Got '{first_word}...'. Use SELECT, WITH, EXPLAIN, DESCRIBE, SHOW, or SUMMARIZE."
        )


class CacheManager:
    """DuckDB-backed cache and analytics engine for Ontario open data."""

    def __init__(self, db_path: str | None = None):
        if db_path is None:
            cache_dir = os.path.expanduser(
                os.environ.get("ONTARIO_DATA_CACHE_DIR", "~/.cache/ontario-data")
            )
            os.makedirs(cache_dir, exist_ok=True)
            db_path = os.path.join(cache_dir, "ontario_data.duckdb")
        self.db_path = db_path
        self.conn = duckdb.connect(db_path)
        self._has_spatial = False

    def initialize(self):
        """Create metadata tables and install extensions."""
        self.conn.execute("""
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
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS _dataset_metadata (
                dataset_id VARCHAR PRIMARY KEY,
                metadata JSON,
                cached_at TIMESTAMP
            )
        """)
        # Install extensions (ignore errors if already installed)
        for ext in ["httpfs", "json"]:
            try:
                self.conn.execute(f"INSTALL {ext}")
                self.conn.execute(f"LOAD {ext}")
            except Exception:
                try:
                    self.conn.execute(f"LOAD {ext}")
                except Exception:
                    pass

        # Spatial extension — track availability
        try:
            self.conn.execute("INSTALL spatial")
            self.conn.execute("LOAD spatial")
            self._has_spatial = True
        except Exception:
            try:
                self.conn.execute("LOAD spatial")
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
        # Drop existing table if re-caching
        if self.is_cached(resource_id):
            old_table = self.get_table_name(resource_id)
            if old_table:
                self.conn.execute(f'DROP TABLE IF EXISTS "{old_table}"')
            self.conn.execute(
                "DELETE FROM _cache_metadata WHERE resource_id = ?", [resource_id]
            )

        # Create table from DataFrame
        self.conn.execute(f'CREATE TABLE "{table_name}" AS SELECT * FROM df')

        # Record metadata
        now = datetime.now(timezone.utc)
        size = df.memory_usage(deep=True).sum()
        self.conn.execute(
            """INSERT INTO _cache_metadata
               (resource_id, dataset_id, table_name, downloaded_at, row_count, size_bytes, source_url)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            [resource_id, dataset_id, table_name, now, len(df), int(size), source_url],
        )

    def is_cached(self, resource_id: str) -> bool:
        """Check if a resource is in the cache."""
        result = self.conn.execute(
            "SELECT 1 FROM _cache_metadata WHERE resource_id = ?", [resource_id]
        ).fetchone()
        return result is not None

    def get_table_name(self, resource_id: str) -> str | None:
        """Get the DuckDB table name for a cached resource."""
        result = self.conn.execute(
            "SELECT table_name FROM _cache_metadata WHERE resource_id = ?", [resource_id]
        ).fetchone()
        return result[0] if result else None

    def list_cached(self) -> list[dict[str, Any]]:
        """List all cached resources."""
        rows = self.conn.execute(
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
        table_name = self.get_table_name(resource_id)
        if table_name:
            self.conn.execute(f'DROP TABLE IF EXISTS "{table_name}"')
        self.conn.execute(
            "DELETE FROM _cache_metadata WHERE resource_id = ?", [resource_id]
        )
    def remove_all(self):
        """Remove all cached resources."""
        for item in self.list_cached():
            self.conn.execute(f"DROP TABLE IF EXISTS \"{item['table_name']}\"")
        self.conn.execute("DELETE FROM _cache_metadata")

    def get_stats(self) -> dict[str, Any]:
        """Get cache statistics."""
        result = self.conn.execute(
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
        result = self.conn.execute(sql)
        columns = [desc[0] for desc in result.description]
        rows = result.fetchall()
        return [dict(zip(columns, row)) for row in rows]

    def query_df(self, sql: str) -> pd.DataFrame:
        """Run a validated read-only SQL query and return a DataFrame."""
        _validate_sql(sql)
        return self.conn.execute(sql).fetchdf()

    def update_expires_at(self, resource_id: str, expires_at):
        """Set the expires_at timestamp for a cached resource."""
        self.conn.execute(
            "UPDATE _cache_metadata SET expires_at = ? WHERE resource_id = ?",
            [expires_at, resource_id],
        )

    def store_dataset_metadata(self, dataset_id: str, metadata: dict[str, Any]):
        """Cache dataset metadata."""
        now = datetime.now(timezone.utc)
        self.conn.execute(
            """INSERT OR REPLACE INTO _dataset_metadata (dataset_id, metadata, cached_at)
               VALUES (?, ?, ?)""",
            [dataset_id, json.dumps(metadata), now],
        )

    def get_dataset_metadata(self, dataset_id: str) -> dict[str, Any] | None:
        """Get cached dataset metadata."""
        result = self.conn.execute(
            "SELECT metadata FROM _dataset_metadata WHERE dataset_id = ?", [dataset_id]
        ).fetchone()
        if result:
            return json.loads(result[0])
        return None

    def close(self):
        """Close the DuckDB connection."""
        self.conn.close()
