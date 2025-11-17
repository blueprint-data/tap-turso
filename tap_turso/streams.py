"""Stream class for tap-turso."""

from typing import Any, Dict, Iterable, Optional, List
from datetime import datetime
import libsql

from singer_sdk.streams import Stream
from singer_sdk import typing as th


class TursoStream(Stream):
    """Stream for Turso SQLite table."""

    def __init__(
        self,
        tap,
        name: str,
        table_name: str,
        replication_method: str = "FULL_TABLE",
        replication_key: Optional[str] = None,
        primary_keys: Optional[List[str]] = None,
        **kwargs,
    ):
        """Initialize TursoStream.

        Args:
            tap: Parent tap instance
            name: Stream name
            table_name: Database table name
            replication_method: Either 'FULL_TABLE' or 'INCREMENTAL'
            replication_key: Column name for incremental replication
            primary_keys: List of primary key column names
        """
        self.table_name = table_name
        self._replication_method = replication_method
        self._replication_key_config = replication_key  # Store configured replication key
        self._primary_keys_config = primary_keys  # Store configured primary keys separately
        self._connection = None
        self._schema_cache = None

        super().__init__(tap=tap, name=name, schema=None, **kwargs)

    @property
    def primary_keys(self) -> Optional[List[str]]:
        """Return primary key(s) for the stream.

        If not explicitly configured, attempts to detect from table schema.
        """
        if self._primary_keys_config is not None:
            return self._primary_keys_config

        # Attempt to detect primary key from table schema
        try:
            detected_pks = self._detect_primary_keys()
            if detected_pks:
                return detected_pks
        except Exception as e:
            self.logger.warning(
                f"Could not detect primary keys for {self.table_name}: {e}"
            )

        return None

    @property
    def replication_key(self) -> Optional[str]:
        """Return replication key for incremental sync."""
        if self._replication_method == "INCREMENTAL":
            return self._replication_key_config
        return None

    @property
    def is_sorted(self) -> bool:
        """Return True if data is sorted by replication key."""
        # We explicitly sort by replication key in our query
        return self._replication_method == "INCREMENTAL"

    @property
    def schema(self) -> dict:
        """Return schema for the stream.

        Dynamically generates schema by inspecting table structure.
        """
        if self._schema_cache is not None:
            return self._schema_cache

        self.logger.info(f"Discovering schema for table: {self.table_name}")
        self._schema_cache = self._discover_schema()
        return self._schema_cache

    def _get_connection(self):
        """Get or create database connection.

        Returns:
            libsql connection object
        """
        if self._connection is not None:
            return self._connection

        config = self.config

        try:
            # Remote connection with embedded replica
            if config.get("sync_url"):
                self.logger.info(
                    f"Connecting to Turso with embedded replica: {config.get('local_path', 'local.db')}"
                )
                self._connection = libsql.connect(
                    database=config.get("local_path", "local.db"),
                    sync_url=config["sync_url"],
                    auth_token=config.get("auth_token"),
                )
                # Sync with remote database
                self._connection.sync()

            # Remote connection only
            elif config.get("database_url"):
                self.logger.info(f"Connecting to remote Turso database")
                # For remote-only, we use embedded replica with temp local file
                self._connection = libsql.connect(
                    database=":memory:",  # Use in-memory for remote-only
                    sync_url=config["database_url"],
                    auth_token=config["auth_token"],
                )
                # Sync to pull data from remote
                self._connection.sync()

            # Local database only
            else:
                self.logger.info(
                    f"Connecting to local database: {config['local_path']}"
                )
                self._connection = libsql.connect(database=config["local_path"])

        except Exception as e:
            self.logger.error(f"Failed to connect to database: {e}")
            raise RuntimeError(f"Database connection failed: {e}")

        return self._connection

    def _detect_primary_keys(self) -> Optional[List[str]]:
        """Detect primary key columns from table schema.

        Returns:
            List of primary key column names, or None if not found
        """
        conn = self._get_connection()

        # Query SQLite table info for primary key
        result = conn.execute(f"PRAGMA table_info({self.table_name})").fetchall()

        primary_keys = []
        for row in result:
            # Row format: (cid, name, type, notnull, dflt_value, pk)
            if row[5] > 0:  # pk column is index 5
                primary_keys.append(row[1])  # name is index 1

        if primary_keys:
            self.logger.info(
                f"Detected primary keys for {self.table_name}: {primary_keys}"
            )
            return primary_keys

        return None

    def _discover_schema(self) -> dict:
        """Discover schema by inspecting table structure.

        Returns:
            Singer schema dictionary
        """
        conn = self._get_connection()

        # Get column information
        result = conn.execute(f"PRAGMA table_info({self.table_name})").fetchall()

        if not result:
            raise ValueError(f"Table '{self.table_name}' not found in database")

        property_list = []

        for row in result:
            # Row format: (cid, name, type, notnull, dflt_value, pk)
            col_name = row[1]
            col_type = row[2].upper()
            is_nullable = row[3] == 0  # notnull: 0 = nullable, 1 = not null
            is_pk = row[5] > 0

            # Map SQLite type to Singer type
            singer_type = self._map_sql_type_to_singer(col_type)

            # Create property with required flag if not nullable and is primary key
            prop = th.Property(col_name, singer_type, required=(not is_nullable and is_pk))
            property_list.append(prop)

        # Add extraction timestamp
        property_list.append(th.Property("_sdc_extracted_at", th.DateTimeType))

        properties = th.PropertiesList(*property_list).to_dict()["properties"]

        schema = {
            "type": "object",
            "properties": properties,
            "additionalProperties": False,
        }

        self.logger.info(
            f"Discovered {len(properties)} columns for table {self.table_name}"
        )

        return schema

    def _map_sql_type_to_singer(self, sql_type: str) -> th.JSONTypeHelper:
        """Map SQLite data type to Singer type.

        Args:
            sql_type: SQLite column type (e.g., 'INTEGER', 'TEXT', 'REAL')

        Returns:
            Singer type helper
        """
        # SQLite type affinity rules
        # See: https://www.sqlite.org/datatype3.html

        sql_type = sql_type.upper()

        # INTEGER types
        if "INT" in sql_type:
            return th.IntegerType

        # TEXT types
        if any(t in sql_type for t in ["CHAR", "CLOB", "TEXT"]):
            # Check for datetime patterns
            if any(t in sql_type for t in ["DATE", "TIME"]):
                return th.DateTimeType
            return th.StringType

        # REAL/NUMERIC types
        if any(t in sql_type for t in ["REAL", "FLOA", "DOUB", "NUMERIC", "DECIMAL"]):
            return th.NumberType

        # BLOB
        if "BLOB" in sql_type:
            return th.StringType  # Encode as base64 string

        # Boolean (SQLite stores as INTEGER 0/1)
        if "BOOL" in sql_type:
            return th.BooleanType

        # Default to string for unknown types
        return th.StringType

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        """Retrieve records from the table.

        Args:
            context: Stream context (includes state)

        Yields:
            Record dictionaries
        """
        conn = self._get_connection()
        batch_size = self.config.get("batch_size", 1000)

        # Build query based on replication method
        if self._replication_method == "INCREMENTAL" and self.replication_key:
            # Incremental query with replication key filter
            query = self._build_incremental_query(context)
        else:
            # Full table query
            query = f"SELECT * FROM {self.table_name}"

        self.logger.info(f"Executing query: {query}")

        # Execute query and fetch in batches
        cursor = conn.execute(query)

        # Get column names from cursor description
        # Note: libsql cursor.description format is [(name, type_code), ...]
        if hasattr(cursor, "description") and cursor.description:
            column_names = [desc[0] for desc in cursor.description]
        else:
            # Fallback: get columns from PRAGMA
            col_info = conn.execute(f"PRAGMA table_info({self.table_name})").fetchall()
            column_names = [row[1] for row in col_info]

        # Fetch and yield records in batches
        record_count = 0
        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break

            for row in rows:
                record = self._row_to_dict(row, column_names)
                record["_sdc_extracted_at"] = datetime.utcnow().isoformat()
                record_count += 1
                yield record

            self.logger.info(f"Fetched {record_count} records from {self.table_name}")

        self.logger.info(
            f"Completed fetching {record_count} total records from {self.table_name}"
        )

    def _build_incremental_query(self, context: Optional[dict]) -> str:
        """Build SQL query for incremental replication.

        Args:
            context: Stream context with state

        Returns:
            SQL query string
        """
        query = f"SELECT * FROM {self.table_name}"

        # Get starting replication key value from state
        start_value = self.get_starting_replication_key_value(context)

        self.logger.info(f"Incremental sync starting from: {start_value}")

        if start_value:
            # Determine if replication key is numeric or string/datetime
            # For safety, we use parameterized-style quoting
            if isinstance(start_value, (int, float)):
                query += f" WHERE {self.replication_key} > {start_value}"
            else:
                # String/datetime - quote the value
                escaped_value = str(start_value).replace("'", "''")
                query += f" WHERE {self.replication_key} > '{escaped_value}'"

        # Order by replication key for consistent state updates
        query += f" ORDER BY {self.replication_key} ASC"

        return query

    def _row_to_dict(self, row: tuple, column_names: List[str]) -> dict:
        """Convert a database row tuple to a dictionary.

        Args:
            row: Database row as tuple
            column_names: List of column names

        Returns:
            Record dictionary
        """
        record = {}
        for i, col_name in enumerate(column_names):
            value = row[i]

            # Handle None values
            if value is None:
                record[col_name] = None
            # Handle datetime conversion if needed
            elif isinstance(value, datetime):
                record[col_name] = value.isoformat()
            # Handle bytes (BLOB) - encode as base64
            elif isinstance(value, bytes):
                import base64

                record[col_name] = base64.b64encode(value).decode("utf-8")
            else:
                record[col_name] = value

        return record

    def __del__(self):
        """Close database connection when stream is destroyed."""
        if self._connection:
            try:
                self._connection.close()
            except Exception:
                pass
