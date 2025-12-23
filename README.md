# YABIT Extension for PostgreSQL

This extension provides a function to fetch tuples from a specified table based on hardcoded positions.

## Installation

1. Navigate to the `postgres/contrib/yabit` directory.
2. Run `make` to build the extension.
3. Run `make install` to install the extension.
4. In PostgreSQL, run `CREATE EXTENSION yabit;` to enable the extension.

## Usage

### Creating a Bitmap Index

YABIT extension provides a bitmap index access method for PostgreSQL. You can create a bitmap index on supported data types (integer, numeric, date) using the following syntax:

```sql
-- Create a bitmap index on an integer column
CREATE INDEX idx_table_column ON table_name USING yabit (column_name);

-- Create a bitmap index on a numeric column
CREATE INDEX idx_table_numeric ON table_name USING yabit (numeric_column);

-- Create a bitmap index on a date column
CREATE INDEX idx_table_date ON table_name USING yabit (date_column);
```

### Using Bitmap Index in Queries

Once created, the PostgreSQL query planner will automatically use the bitmap index when appropriate, especially for:
- Queries with AND/OR conditions
- Queries that benefit from bitmap index combining
- Analytical queries with aggregation

Example:

```sql
-- Query that can use bitmap index
SELECT * FROM table_name WHERE column_name = 100;

-- Query that can benefit from bitmap index combining
SELECT * FROM table_name WHERE column1 = 100 AND column2 > 50;
```

### Helper Functions

The extension also provides some helper functions:

```sql
-- TPCH Query 6 implementation
SELECT tpch_q6('lineitem', 'input.txt', 'debug');

-- View detailed information about an IOV item in a bitmap index
SELECT iovitemdetail('table_name', lov_block, lov_offset);
```

### Bitmap Index Advantages

- Efficient storage for columns with low cardinality
- Fast query performance for complex boolean conditions
- Efficient index combining for multiple conditions
- Suitable for data warehousing and analytical workloads

### Important Notes

#### WAL Support

**Warning**: The YABIT extension currently does not implement Write-Ahead Logging (WAL). This has several important implications:

- **Data durability**: In the event of a crash, bitmap indexes may become corrupted or inconsistent with the base table.
- **Point-in-time recovery**: Bitmap indexes cannot be recovered to a specific point in time using PostgreSQL's PITR feature.
- **Replication**: Bitmap indexes are not replicated to standby servers in streaming replication setups.
- **Backup considerations**: Special care must be taken when backing up databases with bitmap indexes. It is recommended to reindex bitmap indexes after restoring from a backup.
