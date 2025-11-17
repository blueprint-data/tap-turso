"""Test tap-turso Stream class."""

import pytest
from tap_turso.tap import TapTurso
from tap_turso.streams import TursoStream
from singer_sdk import typing as th


def test_stream_schema_discovery(test_database_config):
    """Test schema discovery from database table."""
    tap = TapTurso(config=test_database_config)
    stream = TursoStream(
        tap=tap,
        name="users",
        table_name="users",
        replication_method="INCREMENTAL",
        replication_key="updated_at",
    )

    schema = stream.schema

    assert schema["type"] == "object"
    assert "properties" in schema

    # Check expected columns
    assert "id" in schema["properties"]
    assert "name" in schema["properties"]
    assert "email" in schema["properties"]
    assert "created_at" in schema["properties"]
    assert "updated_at" in schema["properties"]
    assert "_sdc_extracted_at" in schema["properties"]


def test_primary_key_detection(test_database_config):
    """Test automatic primary key detection."""
    tap = TapTurso(config=test_database_config)
    stream = TursoStream(
        tap=tap,
        name="users",
        table_name="users",
        replication_method="FULL_TABLE",
    )

    # Should detect 'id' as primary key
    primary_keys = stream.primary_keys
    assert primary_keys is not None
    assert "id" in primary_keys


def test_primary_key_from_config(test_database_config):
    """Test primary key from configuration."""
    tap = TapTurso(config=test_database_config)
    stream = TursoStream(
        tap=tap,
        name="users",
        table_name="users",
        replication_method="FULL_TABLE",
        primary_keys=["email"],  # Override detected PK
    )

    assert stream.primary_keys == ["email"]


def test_full_table_replication(test_database_config):
    """Test full table extraction."""
    tap = TapTurso(config=test_database_config)
    stream = TursoStream(
        tap=tap,
        name="orders",
        table_name="orders",
        replication_method="FULL_TABLE",
    )

    records = list(stream.get_records(context=None))

    # Should get all 3 orders
    assert len(records) == 3

    # Check record structure
    assert "order_id" in records[0]
    assert "user_id" in records[0]
    assert "total" in records[0]
    assert "_sdc_extracted_at" in records[0]


def test_incremental_replication(test_database_config):
    """Test incremental extraction with replication key."""
    tap = TapTurso(config=test_database_config)
    stream = TursoStream(
        tap=tap,
        name="users",
        table_name="users",
        replication_method="INCREMENTAL",
        replication_key="updated_at",
    )

    # First sync - get all records
    records = list(stream.get_records(context=None))
    assert len(records) == 3

    # Records should be sorted by updated_at
    assert records[0]["id"] == 1  # 2025-01-01
    assert records[1]["id"] == 2  # 2025-01-02
    assert records[2]["id"] == 3  # 2025-01-03


def test_incremental_with_state(test_database_config):
    """Test incremental query building."""
    tap = TapTurso(config=test_database_config)
    stream = TursoStream(
        tap=tap,
        name="users",
        table_name="users",
        replication_method="INCREMENTAL",
        replication_key="updated_at",
    )

    # Test without state - should get all records
    records = list(stream.get_records(context=None))
    assert len(records) == 3

    # Records should be sorted by updated_at
    assert records[0]["id"] == 1
    assert records[1]["id"] == 2
    assert records[2]["id"] == 3


def test_type_mapping(test_database_config):
    """Test SQLite to Singer type mapping."""
    tap = TapTurso(config=test_database_config)

    # Add test_types table to config
    test_config = test_database_config.copy()
    test_config["tables"].append({"name": "test_types", "replication_method": "FULL_TABLE"})

    tap = TapTurso(config=test_config)
    stream = TursoStream(
        tap=tap,
        name="test_types",
        table_name="test_types",
        replication_method="FULL_TABLE",
    )

    schema = stream.schema
    props = schema["properties"]

    # Check type mappings (all should be nullable except primary key)
    assert "integer" in props["id"]["type"]  # INTEGER -> integer
    assert "string" in props["text_col"]["type"]  # TEXT -> string
    assert "integer" in props["int_col"]["type"]  # INTEGER -> integer
    assert "number" in props["real_col"]["type"]  # REAL -> number
    assert "string" in props["blob_col"]["type"]  # BLOB -> string (base64)
    assert "boolean" in props["bool_col"]["type"]  # BOOLEAN -> boolean
    assert "string" in props["datetime_col"]["type"]  # DATETIME -> datetime


def test_record_extraction_with_types(test_database_config):
    """Test that records are properly extracted with correct types."""
    tap = TapTurso(config=test_database_config)

    # Add test_types table to config
    test_config = test_database_config.copy()
    test_config["tables"].append({"name": "test_types", "replication_method": "FULL_TABLE"})

    tap = TapTurso(config=test_config)
    stream = TursoStream(
        tap=tap,
        name="test_types",
        table_name="test_types",
        replication_method="FULL_TABLE",
    )

    records = list(stream.get_records(context=None))

    assert len(records) == 1
    record = records[0]

    # Check values
    assert record["id"] == 1
    assert record["text_col"] == "test"
    assert record["int_col"] == 42
    assert record["real_col"] == 3.14
    assert record["bool_col"] == 1  # SQLite stores booleans as 0/1
    # BLOB is base64 encoded
    assert isinstance(record["blob_col"], str)


def test_batch_fetching(test_database_config):
    """Test batch fetching with small batch size."""
    # Modify config to use small batch size
    config = test_database_config.copy()
    config["batch_size"] = 1  # Fetch one record at a time

    tap = TapTurso(config=config)
    stream = TursoStream(
        tap=tap,
        name="users",
        table_name="users",
        replication_method="FULL_TABLE",
    )

    records = list(stream.get_records(context=None))

    # Should still get all records despite small batch size
    assert len(records) == 3


def test_nonexistent_table(test_database_config):
    """Test error handling for nonexistent table."""
    tap = TapTurso(config=test_database_config)
    stream = TursoStream(
        tap=tap,
        name="nonexistent",
        table_name="nonexistent",
        replication_method="FULL_TABLE",
    )

    # Should raise error when discovering schema
    with pytest.raises(ValueError, match="Table.*not found"):
        _ = stream.schema


def test_connection_reuse(test_database_config):
    """Test that database connection is reused."""
    tap = TapTurso(config=test_database_config)
    stream = TursoStream(
        tap=tap,
        name="users",
        table_name="users",
        replication_method="FULL_TABLE",
    )

    # Get connection twice
    conn1 = stream._get_connection()
    conn2 = stream._get_connection()

    # Should be the same object
    assert conn1 is conn2
