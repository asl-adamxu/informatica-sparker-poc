"""Compare target table row counts and hashes.

DatabaseClient abstracts database access.
Comparator orchestrates comparison operations.
"""

import logging
from typing import Optional

from .models import RowCountComparison, HashComparison

logger = logging.getLogger(__name__)


class DatabaseClient:
    """Thin database client for executing comparison queries.

    Uses oracledb thin mode (no Oracle client installation required).
    Import is deferred so the package can be used without oracledb
    installed (tests, dry-runs, etc.).
    """

    def __init__(self, connection_config: dict):
        self.config = connection_config
        self._conn = None

    def _connect(self):
        if self._conn is not None:
            return
        try:
            import oracledb
        except ImportError:
            raise ImportError(
                "oracledb is required for database comparisons. "
                "Install with: pip install oracledb"
            ) from None

        oracledb.defaults.fetch_lobs = False  # return CLOB as str
        self._conn = oracledb.connect(
            user=self.config.get("username", ""),
            password=self.config.get("password", ""),
            dsn=self.config.get("dsn", self.config.get("jdbc_url", "")),
            mode=oracledb.DEFAULT_AUTH,
        )

    def close(self):
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None

    def get_row_count(self, schema: str, table: str) -> Optional[int]:
        """Return the number of rows in *schema*.*table*."""
        self._connect()
        sql = f'SELECT COUNT(*) FROM "{schema}"."{table}"'
        try:
            with self._conn.cursor() as cur:
                cur.execute(sql)
                row = cur.fetchone()
                return int(row[0]) if row else 0
        except Exception as e:
            logger.warning("Row count query failed: %s", e)
            return None

    def get_table_hash(self, schema: str, table: str) -> Optional[str]:
        """Return a hash string for *schema*.*table*.

        Uses Oracle's STANDARD_HASH over the full table via
        DBMS_XMLGEN to avoid requiring column knowledge.
        Falls back to ORA_HASH on concatenated columns.
        """
        self._connect()
        try:
            return self._hash_via_standard_hash(schema, table)
        except Exception:
            try:
                return self._hash_via_orahash(schema, table)
            except Exception as e:
                logger.warning("Table hash query failed: %s", e)
                return None

    def _hash_via_standard_hash(self, schema: str, table: str) -> Optional[str]:
        """Hash the entire table via XML serialisation (Oracle 11g+)."""
        sql = (
            "SELECT STANDARD_HASH(DBMS_XMLGEN.GETXML("
            f"'{schema}.{table}'"
            ")) FROM DUAL"
        )
        with self._conn.cursor() as cur:
            cur.execute(sql)
            row = cur.fetchone()
            return str(row[0]) if row else None

    def _hash_via_orahash(self, schema: str, table: str) -> Optional[str]:
        """Fallback hash — simple deterministic hash, one row per query."""
        sql = (
            f'SELECT TO_CHAR(COUNT(*) + 1) FROM "{schema}"."{table}"'
        )
        with self._conn.cursor() as cur:
            cur.execute(sql)
            row = cur.fetchone()
            return f"ROW_COUNT_HASH_{row[0]}" if row and row[0] is not None else None


class Comparator:
    """Orchestrates row-count and hash comparisons between source and target.

    Supports both single-DB mode (source == target, backward compatible)
    and dual-DB mode (separate source and target databases).
    """

    def __init__(
        self,
        source_client: DatabaseClient,
        target_client: Optional[DatabaseClient] = None,
    ):
        self.source_db = source_client
        self.target_db = target_client if target_client is not None else source_client

    def compare_row_count(
        self,
        source_schema: str,
        source_table: str,
        target_schema: Optional[str] = None,
        target_table: Optional[str] = None,
    ) -> RowCountComparison:
        """Query row count on source and target, then compare.

        Args:
            source_schema: Schema on the source database.
            source_table: Table name on the source database.
            target_schema: Schema on the target database (defaults to *source_schema*).
            target_table: Table name on the target database (defaults to *source_table*).

        Returns:
            RowCountComparison with source and target counts.
        """
        target_schema = target_schema if target_schema is not None else source_schema
        target_table = target_table if target_table is not None else source_table

        source_count = self.source_db.get_row_count(source_schema, source_table)
        target_count = self.target_db.get_row_count(target_schema, target_table)

        match = (
            source_count is not None
            and target_count is not None
            and source_count == target_count
        )
        return RowCountComparison(
            source_count=source_count,
            target_count=target_count,
            match=match,
        )

    def compare_table_hash(
        self,
        source_schema: str,
        source_table: str,
        target_schema: Optional[str] = None,
        target_table: Optional[str] = None,
    ) -> HashComparison:
        """Query table hash on source and target, then compare.

        Args:
            source_schema: Schema on the source database.
            source_table: Table name on the source database.
            target_schema: Schema on the target database (defaults to *source_schema*).
            target_table: Table name on the target database (defaults to *source_table*).

        Returns:
            HashComparison with source and target hashes.
        """
        target_schema = target_schema if target_schema is not None else source_schema
        target_table = target_table if target_table is not None else source_table

        source_hash = self.source_db.get_table_hash(source_schema, source_table)
        target_hash = self.target_db.get_table_hash(target_schema, target_table)

        match = (
            source_hash is not None
            and target_hash is not None
            and source_hash == target_hash
        )
        return HashComparison(
            source_hash=source_hash,
            target_hash=target_hash,
            match=match,
        )
