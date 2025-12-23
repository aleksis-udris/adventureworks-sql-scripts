# FAQ and Troubleshooting

## Table of Contents
1. [Common Questions](#common-questions)
2. [ETL Issues](#etl-issues)
3. [Query Performance](#query-performance)
4. [Data Quality](#data-quality)
5. [Airflow Problems](#airflow-problems)
6. [ClickHouse Issues](#clickhouse-issues)

---

## Common Questions

### Why do we use ClickHouse instead of PostgreSQL for the warehouse?

ClickHouse is column-oriented, making it much faster for analytical queries:

```
PostgreSQL (row-oriented):
SELECT SUM(revenue) FROM sales WHERE date >= '2025-01-01'
Reads: 100 columns × 10M rows = 1B values

ClickHouse (column-oriented):
Same query
Reads: 1 column × 10M rows = 10M values (100x less data)
```

Result: ClickHouse queries typically 10-100x faster for analytics.

### How often is data updated?

**Incremental loads**: Hourly  
**Data freshness SLA**: 4 hours from transaction  
**Typical timeline**: Orders from yesterday load at 02:00, available by 03:00

### What is the difference between SCD Type 1 and Type 2?

**SCD Type 1** - No history:
```
Before: CustomerID=500, Email=old@example.com
After:  CustomerID=500, Email=new@example.com (overwrites old)
```

**SCD Type 2** - Full history:
```
Before: CustomerKey=1001, CustomerID=500, Email=old@example.com, IsCurrent=1
After:  CustomerKey=1001, CustomerID=500, Email=old@example.com, IsCurrent=0
        CustomerKey=1002, CustomerID=500, Email=new@example.com, IsCurrent=1
```

SCD Type 2 preserves history by creating new rows with surrogate keys.

### How do I query current dimension data vs historical?

**Current data only**:
```sql
SELECT * FROM DimCustomer WHERE IsCurrent = 1;
```

**Historical data as of specific date**:
```sql
SELECT * FROM DimCustomer
WHERE CustomerID = 500
  AND ValidFromDate <= '2025-06-01'
  AND (ValidToDate IS NULL OR ValidToDate > '2025-06-01');
```

### Why are there foreign keys with value 0 in fact tables?

Zero represents unknown or missing dimension. Used when:
- Late-arriving dimension (customer record not loaded yet)
- Source data has NULL foreign key
- Default dimension for error handling

Example:
```sql
-- Customer not found during fact load
CustomerKey = 0  (represents "Unknown Customer")
```

### How long is historical data retained?

Current policy: 2 years of detailed data

Older data:
- Archived to S3
- Or aggregated (daily to monthly summaries)
- Or dropped entirely

Partition drops performed monthly during maintenance window.

---

## ETL Issues

### Problem: DAG shows failed status

**Symptoms**:
- Airflow UI shows red failed task
- Email alert received
- Data not updated

**Diagnosis**:

Check task logs:
```bash
docker exec airflow-webserver airflow tasks logs \
  adventureworks_fact_population \
  extract_resolve_and_load_factsales \
  2025-12-22
```

Check error records:
```sql
SELECT * FROM error_records 
WHERE ErrorDate >= today() 
ORDER BY ErrorDate DESC 
LIMIT 10;
```

**Common Causes**:

1. **Connection timeout**:
   - Network issue between Airflow and database
   - Database overloaded
   - Firewall blocking connection

2. **Foreign key miss**:
   - Dimension isn't loaded before fact
   - Late-arriving dimension
   - Data quality issue in source

3. **Schema change**:
   - Column added/removed in source
   - Data type changed
   - ETL query needs update

**Solutions**:

Connection timeout:
```bash
# Test connectivity
psql -h source-db.company.com -U dwh_reader

# Retry task
airflow tasks test adventureworks_fact_population \
  extract_resolve_and_load_factsales 2025-12-22
```

Foreign key miss:
```bash
# Load dimensions first
airflow dags trigger adventureworks_dimension_sync

# Then retry facts
airflow dags trigger adventureworks_fact_population
```

Schema change:
```bash
# Update query in DAG file
vim dags/adventureworks_fact_tables.py

# Deploy new version
cp dags/adventureworks_fact_tables.py /opt/airflow/dags/

# Trigger DAG
airflow dags trigger adventureworks_fact_population
```

### Problem: ETL running longer than usual

**Normal runtime**: 50 minutes  
**Current runtime**: 3 hours (3.6x slower)

**Diagnosis**:

Check ClickHouse load:
```sql
SELECT 
    query,
    query_duration_ms / 1000 as duration_sec
FROM system.query_log
WHERE event_date = today()
  AND type = 'QueryFinish'
ORDER BY query_duration_ms DESC
LIMIT 10;
```

Check source database:
```sql
-- In PostgreSQL
SELECT * FROM pg_stat_activity 
WHERE state = 'active' 
ORDER BY query_start;
```

**Common Causes**:

1. **Large batch size**: More data than usual
2. **Concurrent queries**: Other users running heavy queries
3. **Partition not pruned**: Query scanning unnecessary data
4. **Disk I/O bottleneck**: Slow disk reads/writes

**Solutions**:

Review partition strategy:
```sql
-- Check partition sizes
SELECT 
    partition,
    formatReadableSize(SUM(bytes_on_disk)) as size
FROM system.parts
WHERE table = 'FactSales' AND active
GROUP BY partition
ORDER BY partition DESC;
```

Kill long-running queries:
```sql
KILL QUERY WHERE query_id = 'slow_query_id';
```

Optimize table:
```sql
OPTIMIZE TABLE FactSales FINAL;
```

### Problem: Recoverable errors not resolving

**Symptoms**:
- Same FK_MISS errors appearing daily
- Error count increasing
- RetryCount > 3 but IsResolved = 0

**Diagnosis**:

```sql
SELECT 
    RecordNaturalKey,
    ErrorMessage,
    RetryCount,
    LastAttemptDate
FROM error_records
WHERE ErrorType = 'FK_MISS'
  AND IsResolved = 0
  AND RetryCount >= 3
ORDER BY ErrorDate DESC;
```

**Common Causes**:

1. **Dimension never loaded**: Source missing customer record
2. **IsCurrent logic wrong**: Old version marked current instead of new
3. **Natural key mismatch**: Different CustomerID in source vs warehouse

**Solutions**:

Verify dimension exists:
```sql
SELECT * FROM DimCustomer WHERE CustomerID = 999;
```

If missing, check source:
```sql
-- In PostgreSQL
SELECT * FROM Sales.Customer WHERE CustomerID = 999;
```

If exists in source, reload dimension:
```bash
airflow dags trigger adventureworks_dimension_sync
```

---

## Query Performance

### Problem: Query taking 30+ seconds

**Expected**: Under 5 seconds  
**Actual**: 35 seconds

**Diagnosis**:

```sql
EXPLAIN
SELECT 
    d.Year,
    SUM(f.SalesRevenue)
FROM FactSales f
JOIN DimDate d ON f.SalesDateKey = d.DateKey
WHERE d.Year = 2025
GROUP BY d.Year;
```

Look for:
- Full table scan instead of partition pruning
- Large number of rows processed
- Memory spills to disk

**Common Causes**:

1. **Function on partition key**:
```sql
-- Bad: Scans all partitions
WHERE YEAR(SalesDateKey) = 2025

-- Good: Uses partition pruning
WHERE SalesDateKey >= '2025-01-01' AND SalesDateKey < '2026-01-01'
```

2. **SELECT * instead of specific columns**:
```sql
-- Bad: Reads 15 columns
SELECT * FROM FactSales

-- Good: Reads 2 columns
SELECT SalesDateKey, SalesRevenue FROM FactSales
```

3. **Large table on right side of join**:
```sql
-- Bad: FactSales (100M rows) on right
SELECT * FROM DimDate d JOIN FactSales f ON d.DateKey = f.SalesDateKey

-- Good: DimDate (3K rows) on right
SELECT * FROM FactSales f JOIN DimDate d ON f.SalesDateKey = d.DateKey
```

**Solutions**:

Rewrite query to enable partition pruning.

Use column selection instead of SELECT *.

Reorder joins to put small table on right.

### Problem: Query returns incorrect counts

**Symptoms**:
- COUNT(*) shows different numbers each time
- Aggregations not matching expected totals
- Historical queries showing current data

**Diagnosis**:

Check for duplicate rows:
```sql
SELECT 
    SalesOrderID,
    SalesOrderDetailID,
    COUNT(*) as duplicate_count
FROM FactSales
GROUP BY SalesOrderID, SalesOrderDetailID
HAVING COUNT(*) > 1;
```

Check SCD current versions:
```sql
SELECT 
    CustomerID,
    COUNT(*) as version_count
FROM DimCustomer
WHERE IsCurrent = 1
GROUP BY CustomerID
HAVING COUNT(*) > 1;
```

**Common Causes**:

1. **Missing IsCurrent filter**:
```sql
-- Wrong: Returns all versions
SELECT * FROM DimCustomer

-- Correct: Returns only current
SELECT * FROM DimCustomer WHERE IsCurrent = 1
```

2. **Duplicate fact loads**:
```sql
-- Check for duplicates
SELECT 
    SalesDateKey,
    COUNT(*) as load_count
FROM FactSales
GROUP BY SalesDateKey
HAVING COUNT(*) > (
    SELECT AVG(cnt) * 1.5
    FROM (
        SELECT SalesDateKey, COUNT(*) as cnt
        FROM FactSales
        GROUP BY SalesDateKey
    )
);
```

**Solutions**:

Add IsCurrent filter:
```sql
SELECT f.*, c.CustomerName
FROM FactSales f
JOIN DimCustomer c ON f.CustomerKey = c.CustomerKey 
WHERE c.IsCurrent = 1;  -- Add this filter
```

Remove duplicates:
```sql
-- Create temp table with distinct rows
CREATE TABLE FactSales_dedup AS
SELECT DISTINCT * FROM FactSales;

-- Drop original
DROP TABLE FactSales;

-- Rename
RENAME TABLE FactSales_dedup TO FactSales;
```

---

## Data Quality

### Problem: NULL values in required columns

**Symptoms**:
- Queries returning NULL for CustomerName
- Aggregations missing data
- Reports showing blank values

**Diagnosis**:

```sql
-- Check NULL counts
SELECT 
    COUNT(*) as total_rows,
    COUNT(CustomerName) as non_null_names,
    COUNT(*) - COUNT(CustomerName) as null_count
FROM DimCustomer;
```

**Common Causes**:

1. **Source data has NULLs**
2. **Foreign key resolution failed**
3. **ETL not applying defaults**

**Solutions**:

Apply default values in transform:
```python
def handle_nulls(row):
    if row['CustomerName'] is None:
        row['CustomerName'] = 'UNKNOWN'
    if row['Email'] is None:
        row['Email'] = ''
    return row
```

Or use COALESCE in queries:
```sql
SELECT COALESCE(CustomerName, 'UNKNOWN') as customer_name
FROM DimCustomer;
```

### Problem: Negative values in amount fields

**Symptoms**:
- SalesRevenue showing negative numbers
- Total revenue less than expected
- Reports showing losses where there should be profits

**Diagnosis**:

```sql
SELECT 
    SalesDateKey,
    SalesRevenue,
    Quantity,
    UnitPrice
FROM FactSales
WHERE SalesRevenue < 0
LIMIT 10;
```

**Common Causes**:

1. **Returns not separated**: Returns mixed with sales
2. **Data quality issue**: Source has incorrect negatives
3. **Calculation error**: Formula producing negative

**Solutions**:

Separate returns into FactReturns table.

Add validation in ETL:
```python
if row['SalesRevenue'] < 0:
    log_error('Negative revenue', row)
    row['SalesRevenue'] = 0  # Or skip row
```

---

## Airflow Problems

### Problem: DAG not appearing in UI

**Diagnosis**:

Check for Python errors:
```bash
docker exec airflow-scheduler airflow dags list-import-errors
```

Check DAG file syntax:
```bash
python -m py_compile dags/my_dag.py
```

**Solutions**:

Fix Python syntax errors.

Verify DAG file in correct directory:
```bash
ls -la /opt/airflow/dags/
```

Restart scheduler:
```bash
docker-compose restart airflow-scheduler
```

### Problem: Tasks stuck in running state

**Symptoms**:
- Task shows running for hours
- No logs being generated
- Cannot clear or restart task

**Diagnosis**:

Check Airflow processes:
```bash
docker exec airflow-scheduler ps aux | grep airflow
```

Check task instance:
```sql
-- In Airflow metadata database
SELECT * FROM task_instance 
WHERE dag_id = 'adventureworks_fact_population'
  AND state = 'running'
  AND start_date < NOW() - INTERVAL '2 hours';
```

**Solutions**:

Mark task as failed manually:
```bash
airflow tasks clear adventureworks_fact_population \
  --task-regex extract_resolve_and_load_factsales \
  --start-date 2025-12-22 \
  --end-date 2025-12-22
```

Restart scheduler:
```bash
docker-compose restart airflow-scheduler
```

---

## ClickHouse Issues

### Problem: Cannot connect to ClickHouse

**Symptoms**:
- Connection timeout
- Authentication failed
- Connection refused

**Diagnosis**:

Test connection:
```bash
clickhouse-client --host clickhouse.company.com --port 9440 --secure --user default --password mypassword --query "SELECT 1"
```

Check service status:
```bash
systemctl status clickhouse-server
```

**Solutions**:

Verify credentials in .env file.

Check firewall:
```bash
telnet clickhouse.company.com 9440
```

Restart service:
```bash
systemctl restart clickhouse-server
```

### Problem: Disk full error

**Symptoms**:
- INSERT queries failing
- Error: "No space left on device"
- System log warnings

**Diagnosis**:

```bash
df -h
```

```sql
SELECT 
    database,
    formatReadableSize(SUM(bytes_on_disk)) as size
FROM system.parts
WHERE active
GROUP BY database;
```

**Solutions**:

Drop old partitions:
```sql
ALTER TABLE FactSales DROP PARTITION '202301';
```

Optimize tables:
```sql
OPTIMIZE TABLE FactSales FINAL;
```

Archive to S3:
```sql
INSERT INTO FUNCTION s3(...)
SELECT * FROM FactSales 
WHERE SalesDateKey < '2023-01-01';
```

---

## Summary

Most common issues:
1. DAG failures: Check logs, verify connections, retry tasks
2. Slow queries: Use partition pruning, select specific columns
3. Data quality: Apply validation, handle NULLs, separate returns
4. Connection issues: Verify credentials, check firewall, restart services
5. Disk full: Drop old partitions, optimize tables, archive data

When stuck:
1. Check error_records table
2. Review Airflow task logs
3. Query ClickHouse system tables
4. Test connections manually
5. Check connections between PostgreSQL tables
6. Run a Select SQL code (Sometimes Clickhouse doesn't 
   show table contents by default)
7. Consult documentation or ask me