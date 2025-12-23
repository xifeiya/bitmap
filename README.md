# YABIT Extension for PostgreSQL

This extension provides a function to fetch tuples from a specified table based on hardcoded positions.

## Installation

1. Navigate to the `postgres/contrib/yabit` directory.
2. Run `make` to build the extension.
3. Run `make install` to install the extension.
4. In PostgreSQL, run `CREATE EXTENSION yabit;` to enable the extension.

## Usage

```sql
SELECT tpch_q6('lineitem', 'input.txt', 'debug');