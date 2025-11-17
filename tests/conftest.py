"""Pytest configuration and fixtures for tap-turso tests."""

import pytest
import sqlite3
import tempfile
import os
from pathlib import Path


@pytest.fixture
def mock_remote_config():
    """Return mock configuration for remote database."""
    return {
        "database_url": "libsql://test-db.turso.io",
        "auth_token": "test_auth_token_12345",
        "tables": [
            {
                "name": "users",
                "replication_method": "INCREMENTAL",
                "replication_key": "updated_at",
                "primary_key": ["id"],
            },
            {
                "name": "orders",
                "replication_method": "FULL_TABLE",
                "primary_key": ["order_id"],
            },
        ],
        "batch_size": 100,
    }


@pytest.fixture
def mock_local_config(tmp_path):
    """Return mock configuration for local database."""
    db_path = tmp_path / "test.db"
    return {
        "local_path": str(db_path),
        "tables": [
            {
                "name": "users",
                "replication_method": "INCREMENTAL",
                "replication_key": "updated_at",
                "primary_key": ["id"],
            },
        ],
        "batch_size": 100,
    }


@pytest.fixture
def test_database(tmp_path):
    """Create a test SQLite database with sample data."""
    db_path = tmp_path / "test.db"

    # Create connection
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()

    # Create users table
    cursor.execute(
        """
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT,
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        )
    """
    )

    # Insert sample data
    cursor.execute(
        """
        INSERT INTO users (id, name, email, created_at, updated_at)
        VALUES
            (1, 'Alice', 'alice@example.com', '2025-01-01 10:00:00', '2025-01-01 10:00:00'),
            (2, 'Bob', 'bob@example.com', '2025-01-02 11:00:00', '2025-01-02 11:00:00'),
            (3, 'Charlie', 'charlie@example.com', '2025-01-03 12:00:00', '2025-01-03 12:00:00')
    """
    )

    # Create orders table
    cursor.execute(
        """
        CREATE TABLE orders (
            order_id INTEGER PRIMARY KEY,
            user_id INTEGER,
            total REAL,
            status TEXT,
            created_at TIMESTAMP
        )
    """
    )

    cursor.execute(
        """
        INSERT INTO orders (order_id, user_id, total, status, created_at)
        VALUES
            (101, 1, 99.99, 'completed', '2025-01-01 15:00:00'),
            (102, 2, 149.50, 'pending', '2025-01-02 16:00:00'),
            (103, 1, 75.00, 'completed', '2025-01-03 17:00:00')
    """
    )

    # Create table with various data types
    cursor.execute(
        """
        CREATE TABLE test_types (
            id INTEGER PRIMARY KEY,
            text_col TEXT,
            int_col INTEGER,
            real_col REAL,
            blob_col BLOB,
            bool_col BOOLEAN,
            datetime_col DATETIME
        )
    """
    )

    cursor.execute(
        """
        INSERT INTO test_types (id, text_col, int_col, real_col, blob_col, bool_col, datetime_col)
        VALUES (1, 'test', 42, 3.14, X'48656c6c6f', 1, '2025-01-01 10:00:00')
    """
    )

    conn.commit()
    conn.close()

    return db_path


@pytest.fixture
def test_database_config(test_database):
    """Return configuration for test database."""
    return {
        "local_path": str(test_database),
        "tables": [
            {
                "name": "users",
                "replication_method": "INCREMENTAL",
                "replication_key": "updated_at",
            },
            {
                "name": "orders",
                "replication_method": "FULL_TABLE",
            },
        ],
        "batch_size": 100,
    }


@pytest.fixture(autouse=True)
def reset_connection_cache():
    """Reset connection cache between tests."""
    yield
    # Cleanup happens automatically with stream destruction
