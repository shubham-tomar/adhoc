# Vacuum Manager for PostgreSQL Partitions

This utility enables or disables vacuum operations for all partitions of a specified PostgreSQL table. It's particularly useful for managing vacuum operations during batch inserts or other resource-intensive database operations.

## Features

- Recursively finds all partitions and sub-partitions of a parent table
- Enables or disables autovacuum for all identified partitions
- Handles the operations transactionally (all succeed or all fail)
- Configurable through command-line flags

## Usage

```bash
# Enable vacuum (default)
go run main.go --parent=asset_combination --schema=check2 --enable=true

# Disable vacuum
go run main.go --parent=asset_combination --schema=check2 --enable=false

# With custom database connection
go run main.go --host=myhost --port=5432 --user=myuser --password=mypass --dbname=mydb --parent=asset_combination
```

## Command Line Options

| Flag        | Default                                            | Description                            |
|-------------|----------------------------------------------------|-----------------------------------------|
| --host      | dataplatform-dev-rds.c7kssuaisap5.us-east-1.rds.amazonaws.com | Database host                         |
| --port      | 5432                                               | Database port                          |
| --user      | postgres                                           | Database user                          |
| --password  | 1Eu7vK9dZeqrqMs6                                     | Database password                      |
| --dbname    | optimization_dev_pse                                | Database name                          |
| --parent    | asset_combination                                  | Parent table name                      |
| --schema    | check2                                             | Schema name                            |
| --enable    | true                                               | true to enable vacuum, false to disable|
| --timeout   | 5m                                                 | Timeout for operations                 |
| --verbose   | false                                              | Enable verbose output                  |

## Examples

### Disable vacuum before batch inserts

```bash
# Disable vacuum on all partitions of asset_combination
go run main.go --parent=asset_combination --schema=check2 --enable=false

# Run your batch insert operations
# ...

# Re-enable vacuum when done
go run main.go --parent=asset_combination --schema=check2 --enable=true
```

### Process multiple tables

```bash
# Disable vacuum for multiple tables
go run main.go --parent=asset_combination --schema=check2 --enable=false
go run main.go --parent=metric_raw_value --schema=check2 --enable=false

# Perform operations
# ...

# Re-enable vacuum
go run main.go --parent=asset_combination --schema=check2 --enable=true
go run main.go --parent=metric_raw_value --schema=check2 --enable=true
```
