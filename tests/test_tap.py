"""Test tap-turso Tap class."""

import pytest
from tap_turso.tap import TapTurso


@pytest.mark.skip(reason="Requires actual Turso remote database connection")
def test_tap_initialization_with_remote_config(mock_remote_config):
    """Test tap initialization with valid remote configuration."""
    tap = TapTurso(config=mock_remote_config)
    assert tap.config["database_url"] == "libsql://test-db.turso.io"
    assert tap.config["auth_token"] == "test_auth_token_12345"
    assert len(tap.config["tables"]) == 2


def test_tap_initialization_with_local_config(test_database_config):
    """Test tap initialization with valid local configuration."""
    tap = TapTurso(config=test_database_config)
    assert "local_path" in tap.config
    assert len(tap.config["tables"]) == 2


def test_tap_missing_connection_config():
    """Test that tap fails without database_url or local_path."""
    config = {
        "tables": [{"name": "users", "replication_method": "FULL_TABLE"}],
    }

    with pytest.raises(ValueError, match="Must provide either"):
        tap = TapTurso(config=config)
        tap._validate_config()


def test_tap_both_connection_configs():
    """Test that tap fails with both database_url and local_path."""
    config = {
        "database_url": "libsql://test.turso.io",
        "local_path": "/tmp/test.db",
        "auth_token": "token",
        "tables": [{"name": "users"}],
    }

    with pytest.raises(ValueError, match="Cannot provide both"):
        tap = TapTurso(config=config)
        tap._validate_config()


def test_tap_missing_auth_token():
    """Test that tap fails when database_url is provided without auth_token."""
    config = {
        "database_url": "libsql://test.turso.io",
        "tables": [{"name": "users"}],
    }

    with pytest.raises(ValueError, match="auth_token.*required"):
        tap = TapTurso(config=config)
        tap._validate_config()


def test_tap_incremental_without_replication_key():
    """Test that tap fails when INCREMENTAL is set without replication_key."""
    config = {
        "local_path": "/tmp/test.db",
        "tables": [{"name": "users", "replication_method": "INCREMENTAL"}],
    }

    with pytest.raises(ValueError, match="replication_key.*specified"):
        tap = TapTurso(config=config)
        tap._validate_config()


def test_stream_discovery(test_database_config):
    """Test stream discovery from configuration."""
    tap = TapTurso(config=test_database_config)
    streams = tap.discover_streams()

    assert len(streams) == 2

    stream_names = [s.name for s in streams]
    assert "users" in stream_names
    assert "orders" in stream_names

    # Check users stream (incremental)
    users_stream = next(s for s in streams if s.name == "users")
    assert users_stream.replication_key == "updated_at"
    assert users_stream.table_name == "users"

    # Check orders stream (full table)
    orders_stream = next(s for s in streams if s.name == "orders")
    assert orders_stream.replication_key is None
    assert orders_stream.table_name == "orders"


def test_tap_with_custom_batch_size(test_database_config):
    """Test tap with custom batch size configuration."""
    test_database_config["batch_size"] = 500
    tap = TapTurso(config=test_database_config)
    assert tap.config["batch_size"] == 500
