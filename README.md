# tap-turso

Singer tap for Turso SQLite databases, built with the Meltano Singer SDK.

## Features

- Extracts data from Turso SQLite databases (local and remote)
- Supports both **FULL_TABLE** and **INCREMENTAL** replication methods
- Dynamic schema discovery from table structure
- Automatic primary key detection
- Configurable batch sizes for large tables
- Support for embedded replicas (local-first with remote sync)
- Multiple table extraction with independent replication settings

## Installation

```bash
pip install tap-turso
```

Or with pipx:

```bash
pipx install tap-turso
```

For development:

```bash
cd tap-turso
poetry install
```

## Configuration

### Required Settings

You must provide **one** of the following connection methods:

**Option 1: Remote Database**
- `database_url` (string): Turso database URL (e.g., `libsql://your-db.turso.io`)
- `auth_token` (string, secret): Turso authentication token

**Option 2: Local Database**
- `local_path` (string): Path to local SQLite database file

**Option 3: Embedded Replica**
- `local_path` (string): Path to local database file
- `sync_url` (string): Remote Turso database URL for syncing
- `auth_token` (string, secret): Turso authentication token

**Table Configuration**
- `tables` (array, required): List of tables to extract with their settings

### Optional Settings

- `batch_size` (integer, default: 1000): Number of records to fetch per batch

### Table Configuration Schema

Each table in the `tables` array supports:

- `name` (string, required): Table name to extract
- `replication_method` (string, default: "FULL_TABLE"): Either "FULL_TABLE" or "INCREMENTAL"
- `replication_key` (string): Column name for incremental replication (required if `replication_method` is "INCREMENTAL")
- `primary_key` (array of strings): List of primary key column names (auto-detected if not specified)

### Authentication

**Turso Cloud Authentication:**

1. Create a Turso database: `turso db create my-database`
2. Get database URL: `turso db show my-database --url`
3. Create auth token: `turso db tokens create my-database`
4. Use the URL and token in your configuration

**Local Development:**

For local SQLite databases, simply provide the file path in `local_path`.

### Configuration Examples

#### Example 1: Remote Turso Database

```json
{
  "database_url": "libsql://my-database-turso.turso.io",
  "auth_token": "eyJhbGc...",
  "tables": [
    {
      "name": "users",
      "replication_method": "INCREMENTAL",
      "replication_key": "updated_at"
    },
    {
      "name": "orders",
      "replication_method": "INCREMENTAL",
      "replication_key": "modified_at",
      "primary_key": ["order_id"]
    },
    {
      "name": "products",
      "replication_method": "FULL_TABLE"
    }
  ],
  "batch_size": 500
}
```

#### Example 2: Local SQLite Database

```json
{
  "local_path": "/path/to/database.db",
  "tables": [
    {
      "name": "customers",
      "replication_method": "INCREMENTAL",
      "replication_key": "updated_at"
    },
    {
      "name": "transactions",
      "replication_method": "FULL_TABLE"
    }
  ]
}
```

#### Example 3: Embedded Replica (Local-First with Remote Sync)

```json
{
  "local_path": "./local-replica.db",
  "sync_url": "libsql://my-database-turso.turso.io",
  "auth_token": "eyJhbGc...",
  "tables": [
    {
      "name": "users",
      "replication_method": "INCREMENTAL",
      "replication_key": "updated_at"
    }
  ]
}
```

## Streams

Streams are dynamically created based on your `tables` configuration. Each table becomes a stream with:

- **Schema**: Auto-discovered from SQLite table structure
- **Primary Keys**: Auto-detected from table schema or configured manually
- **Replication Method**: FULL_TABLE or INCREMENTAL as configured
- **Replication Key**: For incremental syncs, tracks the bookmark value

### Type Mapping

SQLite types are mapped to Singer types as follows:

| SQLite Type | Singer Type | Notes |
|-------------|-------------|-------|
| INTEGER, INT | integer | Whole numbers |
| TEXT, VARCHAR, CHAR | string | Text data |
| REAL, FLOAT, DOUBLE | number | Decimal numbers |
| NUMERIC, DECIMAL | number | Decimal numbers |
| BLOB | string | Base64-encoded |
| BOOLEAN, BOOL | boolean | Stored as 0/1 in SQLite |
| DATE, DATETIME, TIMESTAMP | string | ISO 8601 format |

## Usage

### With Meltano

Add to your `meltano.yml`:

```yaml
plugins:
  extractors:
    - name: tap-turso
      pip_url: tap-turso
      config:
        database_url: ${TURSO_DATABASE_URL}
        auth_token: ${TURSO_AUTH_TOKEN}
        tables:
          - name: users
            replication_method: INCREMENTAL
            replication_key: updated_at
          - name: orders
            replication_method: FULL_TABLE
```

Run extraction:

```bash
meltano run tap-turso target-jsonl
```

### Standalone

```bash
tap-turso --config config.json | target-jsonl
```

Or with state for incremental syncs:

```bash
tap-turso --config config.json --state state.json | target-jsonl > output.jsonl
```

## Replication Methods

### FULL_TABLE

Extracts all records from the table on every run. Use this for:
- Small tables that change frequently
- Tables without a reliable replication key
- Dimension tables

```json
{
  "name": "products",
  "replication_method": "FULL_TABLE"
}
```

### INCREMENTAL

Extracts only new or updated records based on a replication key. Use this for:
- Large tables with millions of rows
- Tables with an `updated_at`, `modified_at`, or similar timestamp column
- Fact tables and event logs

Requirements:
- Table must have a replication key column (timestamp or incrementing integer)
- Column must be indexed for good performance

```json
{
  "name": "events",
  "replication_method": "INCREMENTAL",
  "replication_key": "created_at"
}
```

**How it works:**
1. First run extracts all records
2. Tap saves the maximum replication key value in state
3. Subsequent runs only extract records where `replication_key > last_saved_value`
4. Records are ordered by replication key for consistent state updates

## Development

### Prerequisites

- Python 3.9+
- Poetry

### Setup

```bash
# Clone repository
git clone https://github.com/blueprintdata/tap-turso
cd tap-turso

# Install dependencies
poetry install

# Run tests
poetry run pytest

# Run with coverage
poetry run pytest --cov=tap_turso
```

### Testing

```bash
# Run all tests
poetry run pytest

# Run specific test file
poetry run pytest tests/test_streams.py

# Run with verbose output
poetry run pytest -v

# Run with coverage report
poetry run pytest --cov=tap_turso --cov-report=html
```

### Code Quality

```bash
# Format code
poetry run black tap_turso tests

# Lint code
poetry run flake8 tap_turso tests
```

## Known Limitations

- **Connection Pooling**: Each stream creates its own connection. For many tables, consider connection limits.
- **Large BLOBs**: BLOB columns are base64-encoded, which increases size by ~33%.
- **Sync Performance**: Remote databases require network roundtrips. Use embedded replicas for better performance.
- **SQLite Limitations**: Inherits SQLite's concurrency limitations (one writer at a time).

## Troubleshooting

### "Table not found" error

- Verify table name is correct (case-sensitive)
- Check database connection is valid
- Ensure table exists: `sqlite3 database.db ".tables"`

### "auth_token is required" error

- Remote connections require an auth token
- Get token from Turso: `turso db tokens create <database-name>`

### Incremental sync not working

- Verify `replication_key` column exists in table
- Check column has appropriate data type (timestamp/integer)
- Ensure records are actually being updated

### Performance issues

- Reduce `batch_size` if running out of memory
- Increase `batch_size` for faster extraction of large tables
- Use embedded replicas for remote databases
- Add indexes on replication key columns

## Changelog

See [CHANGELOG.md](CHANGELOG.md) for version history.

## License

Apache 2.0 - See [LICENSE](LICENSE) for details.

## Support

- Report issues: https://github.com/blueprintdata/tap-turso/issues
- Turso documentation: https://docs.turso.tech
- Singer SDK documentation: https://sdk.meltano.com

## Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

## Credits

Built with the [Meltano Singer SDK](https://sdk.meltano.com) for [Turso](https://turso.tech) SQLite databases.
