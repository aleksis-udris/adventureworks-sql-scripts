# Glossary & Terminology

## Table of Contents
1. [Data Warehousing Terms](#data-warehousing-terms)
2. [ClickHouse Concepts](#clickhouse-concepts)
3. [Airflow Terminology](#airflow-terminology)
4. [ETL Concepts](#etl-concepts)
5. [Performance Terms](#performance-terms)
6. [Acronyms](#acronyms)

---

## Data Warehousing Terms

### Aggregate Table
Pre-computed summary table storing aggregated measures (SUM, COUNT, AVG) at a coarser grain than fact tables. Used to speed up common queries.

**Example**: agg_daily_sales stores daily totals instead of scanning FactSales line items.

### Conformed Dimension
A dimension table shared across multiple fact tables, ensuring consistent definitions and relationships.

**Example**: DimProduct used by FactSales, FactPurchases, and FactInventory.

### Data Mart
Subset of data warehouse focused on specific business area or department.

**Example**: ADVENTUREWORKS_DS (Data Store) is a data mart for pre-aggregated reporting.

### Data Warehouse
Central repository for integrated data from multiple sources, optimized for analysis and reporting.

**Example**: ADVENTUREWORKS_DWS (Data Warehouse Store).

### Degenerate Dimension
Dimension attribute stored directly in fact table without separate dimension table, typically high-cardinality identifiers.

**Example**: InvoiceNumber in FactFinance, UnitPrice in FactSales.

### Dimension Table
Reference table containing descriptive attributes for analysis. Typically small, slowly changing.

**Example**: DimCustomer, DimProduct, DimDate.

### Fact Table
Central table in star schema containing measurable business events and foreign keys to dimensions.

**Example**: FactSales, FactPurchases, FactInventory.

### Grain
Level of detail in a fact table, defining what each row represents.

**Example**: FactSales grain = one row per order line item.

### Natural Key
Business identifier from source system used to uniquely identify entity.

**Example**: CustomerID in source system.

### SCD (Slowly Changing Dimension)
Methodology for tracking changes to dimension attributes over time.

**Types**:
- **Type 1**: Overwrite (no history)
- **Type 2**: Full history with versioning
- **Type 3**: Limited history (previous value only)

### Star Schema
Database design with central fact tables surrounded by dimension tables, resembling a star shape.

**Benefits**: Simple, fast queries, intuitive for analysts.

### Surrogate Key
System-generated unique identifier (typically integer) used as primary key, independent of business keys.

**Example**: CustomerKey (surrogate) vs CustomerID (natural).

---

## ClickHouse Concepts

### Adaptive Granularity
ClickHouse feature that automatically adjusts index granularity based on data characteristics.

### Codec
Compression algorithm applied to columns to reduce storage.

**Examples**:
- ZSTD: General purpose compression
- Delta: For sequential numbers
- Gorilla: For floating point numbers

### Column-Oriented Storage
Database storage format where each column is stored separately, enabling fast analytical queries by reading only needed columns.

**vs Row-Oriented**: PostgreSQL stores entire rows together, requires reading all columns.

### Distributed Table
Virtual table that routes queries across multiple ClickHouse servers in a cluster.

**Syntax**: ENGINE = Distributed(cluster_name, database, table, sharding_key)

### MergeTree Engine Family
ClickHouse table engines optimized for high-volume inserts and fast queries.

**Variants**:
- **MergeTree**: Basic sorted table
- **ReplacingMergeTree**: Deduplicates rows by version
- **SummingMergeTree**: Automatically sums numeric columns
- **AggregatingMergeTree**: Stores aggregation states

### Partition
Physical subdivision of table data, typically by time period (month, day).

**Benefits**: Fast data pruning, easy to drop old data.

### Partition Pruning
Optimization where query only scans relevant partitions, skipping others.

**Example**: Query with WHERE SalesDateKey = '2025-11-11' only scans November 2025 partition.

### Primary Key / ORDER BY
Defines sort order on disk and creates sparse index for fast lookups.

**Note**: In ClickHouse, ORDER BY is more important than PRIMARY KEY (they're often the same).

### Sparse Index
Index that stores values at intervals (every N rows) rather than every row, using less memory.

**Default**: Index mark every 8,192 rows.

### TTL (Time To Live)
Automatic data expiration rule, can delete old data or move to cheaper storage.

**Example**: DELETE rows WHERE date < today() - 90

---

## Airflow Terminology

### DAG (Directed Acyclic Graph)
Workflow definition in Airflow, collection of tasks with dependencies, no cycles.

**Example**: adventureworks_dimension_sync DAG.

### DAG Run
Single execution instance of a DAG for a specific date/time.

**Example**: DAG run for 2025-11-11 02:00:00.

### Executor
Component that runs tasks, determines where/how tasks execute.

**Types**:
- **LocalExecutor**: Tasks run on same machine as scheduler
- **CeleryExecutor**: Tasks distributed across worker nodes
- **KubernetesExecutor**: Tasks run in Kubernetes pods

### Fernet Key
Encryption key used by Airflow to encrypt sensitive data (passwords, connections).

### Operator
Task type in Airflow that performs specific action.

**Examples**:
- PythonOperator: Run Python function
- BashOperator: Run shell command
- SqlOperator: Execute SQL query

### Task
Single unit of work in DAG, instance of an operator.

**Example**: extract_resolve_and_load_factsales task.

### Task Instance
Single execution of a task for specific DAG run.

**Example**: extract_resolve_and_load_factsales task in 2025-11-11 02:00:00 DAG run.

### XCom (Cross-Communication)
Mechanism for tasks to exchange small amounts of data.

**Limitation**: Max 48KB, stored in Airflow metadata database.

---

## ETL Concepts

### Batch Processing
Processing data in groups (batches) rather than one record at a time.

**Example**: Insert 10,000 rows at once instead of 10,000 individual inserts.

### Bulk Lookup
Resolving multiple foreign keys in single query instead of one-by-one.

**Example**: Lookup all CustomerKeys for 50,000 rows in one query.

### Catchup
Airflow feature to run missed DAG executions when DAG is enabled.

**Best Practice**: Set catchup=False for data warehouses to avoid backfilling.

### Data Freshness
Time lag between data creation in source and availability in warehouse.

**SLA**: Data available within 4 hours of transaction.

### ELT (Extract Load Transform)
Load raw data first, transform in warehouse.

**vs ETL**: Transform before loading (our approach).

### Error Recovery
Process of identifying and reprocessing failed records.

**Example**: Retry FK_MISS errors after dimension loads.

### Incremental Load
Loading only new or changed data since last run.

**vs Full Load**: Reload entire dataset every time.

### Late-Arriving Dimension
Fact record arrives before corresponding dimension record.

**Solution**: Default dimension key or reprocessing.

### Reconciliation
Comparing row counts and data between source and warehouse to ensure completeness.

---

## Performance Terms

### Cardinality
Number of unique values in a column.

**High Cardinality**: CustomerID (19,000 unique)
**Low Cardinality**: Gender (2-3 unique)

### Compression Ratio
Ratio of uncompressed to compressed data size.

**Example**: 100 GB uncompressed → 10 GB compressed = 10:1 ratio

### Horizontal Scaling (Scale Out)
Adding more servers to distribute load.

**Example**: 2-node ClickHouse cluster instead of 1 node.

### Index Granularity
Number of rows between index marks in sparse index.

**Default**: 8,192 rows

### Merge
ClickHouse background process combining small data parts into larger ones.

**Trigger**: OPTIMIZE TABLE command or automatic.

### OLAP (Online Analytical Processing)
Workload focused on complex queries, aggregations, and analytics.

**vs OLTP**: Transactional workload with simple queries, many updates.

### Part
Physical data file in ClickHouse partition, created by each insert.

**Problem**: Too many parts (>100) slow down queries.
**Solution**: OPTIMIZE TABLE to merge parts.

### Query Cache
Storing query results to return instantly on repeat execution.

**Validity**: Cache invalidated when underlying data changes.

### Vertical Scaling (Scale Up)
Adding more resources (CPU, RAM) to existing server.

**Example**: Upgrade from 16GB to 32GB RAM.

---

## Acronyms

**ACID**: Atomicity, Consistency, Isolation, Durability - database transaction properties

**CLI**: Command Line Interface

**CPU**: Central Processing Unit

**CSV**: Comma-Separated Values

**DAG**: Directed Acyclic Graph (Airflow workflow)

**DDL**: Data Definition Language (CREATE, ALTER, DROP)

**DML**: Data Manipulation Language (SELECT, INSERT, UPDATE, DELETE)

**DWH**: Data Warehouse

**ETL**: Extract, Transform, Load

**ELT**: Extract, Load, Transform

**FK**: Foreign Key

**GB**: Gigabyte

**I/O**: Input/Output (disk operations)

**MB**: Megabyte

**OLAP**: Online Analytical Processing

**OLTP**: Online Transaction Processing

**PK**: Primary Key

**RAM**: Random Access Memory

**RDBMS**: Relational Database Management System

**ROI**: Return on Investment

**SCD**: Slowly Changing Dimension

**SDK**: Software Development Kit

**SLA**: Service Level Agreement

**SQL**: Structured Query Language

**SSD**: Solid State Drive

**TB**: Terabyte

**TTL**: Time To Live

**UI**: User Interface

**UUID**: Universally Unique Identifier

**WSL**: Windows Subsystem for Linux

**XCom**: Cross-Communication (Airflow)

---

## Data Types

### ClickHouse Data Types

**Integers**:
- Int8, Int16, Int32, Int64: Signed integers
- UInt8, UInt16, UInt32, UInt64: Unsigned integers

**Floating Point**:
- Float32, Float64: Approximate decimals
- Decimal(P, S): Exact decimals (P=precision, S=scale)

**Strings**:
- String: Variable length
- FixedString(N): Fixed length N bytes
- LowCardinality(String): Dictionary encoding for repeated values

**Dates**:
- Date: Calendar date (no time)
- DateTime: Date with time (second precision)
- DateTime64: Date with time (sub-second precision)

**Special**:
- Nullable(T): Allows NULL values (has performance cost)
- Array(T): Array of type T
- Tuple(T1, T2, ...): Multiple types combined

---

## Query Patterns

### Anti-Patterns (Avoid)

**SELECT * **:
```sql
-- Bad: Reads all columns
SELECT * FROM FactSales;

-- Good: Select only needed columns
SELECT SalesDateKey, SalesRevenue FROM FactSales;
```

**Function on Partition Key**:
```sql
-- Bad: Prevents partition pruning
WHERE YEAR(SalesDateKey) = 2025

-- Good: Enables partition pruning
WHERE SalesDateKey >= '2025-01-01' AND SalesDateKey < '2026-01-01'
```

**Missing IsCurrent Filter**:
```sql
-- Bad: Returns all versions
SELECT * FROM DimCustomer WHERE CustomerID = 500

-- Good: Returns current version only
SELECT * FROM DimCustomer WHERE CustomerID = 500 AND IsCurrent = 1
```

### Best Practices

**Partition Filtering**:
```sql
-- Always filter on partition key
WHERE SalesDateKey >= '2025-11-01'
```

**Column Selectivity**:
```sql
-- Select specific columns
SELECT SalesDateKey, ProductKey, SalesRevenue
```

**Join Optimization**:
```sql
-- Small dimension on right side
FROM FactSales fs
JOIN DimDate d ON fs.SalesDateKey = d.DateKey
```

**Aggregation Pre-computation**:
```sql
-- Use aggregate tables for common queries
FROM agg_daily_sales  -- Instead of FactSales
```

---

## Error Codes

### Common Error Types

**FK_MISS**: Foreign key not found in dimension table
- **Recoverable**: Yes
- **Action**: Retry after dimension load

**NULL_PK**: Primary key is NULL
- **Recoverable**: No
- **Action**: Fix source data

**TYPE_MISMATCH**: Data type conversion failed
- **Recoverable**: Sometimes
- **Action**: Clean source data or adjust target type

**DUPLICATE_PK**: Duplicate primary key insert
- **Recoverable**: Yes
- **Action**: Skip duplicate, log warning

**VALIDATION_FAIL**: Business rule validation failed
- **Recoverable**: Yes
- **Action**: Review business logic, may skip

**TIMEOUT**: Query or connection timeout
- **Recoverable**: Yes
- **Action**: Retry with backoff

**SCHEMA_MISMATCH**: Expected column missing or wrong type
- **Recoverable**: No
- **Action**: Fix schema or query

---

## Summary

**Key Concepts to Remember**:
- **Grain**: Level of detail in fact table
- **SCD Type 2**: Full history preservation with versioning
- **Partition Pruning**: Scanning only relevant partitions
- **Surrogate Key**: System-generated identifier
- **Star Schema**: Facts surrounded by dimensions

**Performance Priorities**:
1. Partition pruning
2. Column selection
3. Pre-aggregation
4. Index usage
5. Compression

**Next Steps**:
- See [Schema Specification](schema_specification.md) for table definitions
- See [FAQ & Troubleshooting](faq_troubleshooting.md) for common issues
- See [Performance & Capacity Planning](performance_capacity.md) for optimization