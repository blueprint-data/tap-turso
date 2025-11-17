"""Turso tap class."""

from typing import List

from singer_sdk import Stream, Tap
from singer_sdk import typing as th

from tap_turso.streams import TursoStream


class TapTurso(Tap):
    """Singer tap for Turso SQLite databases."""

    name = "tap-turso"

    config_jsonschema = th.PropertiesList(
        # Connection settings
        th.Property(
            "database_url",
            th.StringType,
            required=False,
            description="Turso database URL for remote connection (e.g., libsql://your-db.turso.io). "
            "Required if using remote database. Mutually exclusive with local_path.",
        ),
        th.Property(
            "auth_token",
            th.StringType,
            required=False,
            secret=True,
            description="Turso authentication token. Required when using database_url.",
        ),
        th.Property(
            "local_path",
            th.StringType,
            required=False,
            description="Path to local SQLite database file. "
            "Mutually exclusive with database_url.",
        ),
        th.Property(
            "sync_url",
            th.StringType,
            required=False,
            description="Remote sync URL for embedded replicas (optional). "
            "Enables local-first mode with remote sync.",
        ),
        # Table configuration
        th.Property(
            "tables",
            th.ArrayType(
                th.ObjectType(
                    th.Property(
                        "name",
                        th.StringType,
                        required=True,
                        description="Table name to extract",
                    ),
                    th.Property(
                        "replication_method",
                        th.StringType,
                        allowed_values=["FULL_TABLE", "INCREMENTAL"],
                        default="FULL_TABLE",
                        description="Replication method for this table",
                    ),
                    th.Property(
                        "replication_key",
                        th.StringType,
                        description="Column name to use for incremental replication "
                        "(required if replication_method is INCREMENTAL)",
                    ),
                    th.Property(
                        "primary_key",
                        th.ArrayType(th.StringType),
                        description="List of column names that form the primary key. "
                        "If not specified, will attempt to detect from table schema.",
                    ),
                )
            ),
            required=True,
            description="List of tables to extract with their replication settings",
        ),
        # Performance settings
        th.Property(
            "batch_size",
            th.IntegerType,
            default=1000,
            description="Number of records to fetch per batch",
        ),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams.

        Returns:
            List of TursoStream instances, one per configured table.
        """
        streams = []

        for table_config in self.config.get("tables", []):
            stream = TursoStream(
                tap=self,
                name=table_config["name"],
                table_name=table_config["name"],
                replication_method=table_config.get("replication_method", "FULL_TABLE"),
                replication_key=table_config.get("replication_key"),
                primary_keys=table_config.get("primary_key"),
            )
            streams.append(stream)

        return streams

    def _validate_config(self, raise_errors: bool = True) -> None:
        """Validate tap configuration."""
        super()._validate_config(raise_errors=raise_errors)

        # Check that either database_url, sync_url, or local_path is provided
        has_database_url = self.config.get("database_url")
        has_sync_url = self.config.get("sync_url")
        has_local_path = self.config.get("local_path")

        # Embedded replica mode: local_path + sync_url
        is_embedded_replica = has_local_path and has_sync_url

        # Remote only mode: database_url (no local_path or sync_url)
        is_remote_only = has_database_url and not has_local_path and not has_sync_url

        # Local only mode: local_path (no database_url or sync_url)
        is_local_only = has_local_path and not has_database_url and not has_sync_url

        if not (is_embedded_replica or is_remote_only or is_local_only):
            raise ValueError(
                "Must provide one of: "
                "1) 'local_path' + 'sync_url' (embedded replica), "
                "2) 'database_url' (remote only), or "
                "3) 'local_path' (local only)"
            )

        # Check that auth_token is provided when using remote connection
        if (has_database_url or has_sync_url) and not self.config.get("auth_token"):
            raise ValueError(
                "'auth_token' is required when using 'database_url' or 'sync_url' for remote connection"
            )

        # Validate table configurations
        for table_config in self.config.get("tables", []):
            if table_config.get("replication_method") == "INCREMENTAL":
                if not table_config.get("replication_key"):
                    raise ValueError(
                        f"Table '{table_config['name']}' has replication_method='INCREMENTAL' "
                        "but no 'replication_key' is specified"
                    )
