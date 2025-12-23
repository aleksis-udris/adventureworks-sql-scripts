# Performance & Capacity Planning

## Table of Contents
1. [Query Optimization](#query-optimization)
2. [Partition Management](#partition-management)
3. [Index Strategy](#index-strategy)
4. [Capacity Planning](#capacity-planning)
5. [Performance Monitoring](#performance-monitoring)
6. [Optimization Techniques](#optimization-techniques)

---

## Query Optimization

### Partition Pruning

**Most Important Optimization**: Always filter on partition key to enable partition pruning.

**Bad Query** (scans all partitions):
```sql
-- Scans 100M rows across all partitions
SELECT * FROM FactSales 
WHERE YEAR(SalesDateKey) = 2025;

-- Execution: 15 seconds
-- Partitions scanned: 365 (all days of 2025)
```

**Good Query** (prunes partitions):
```sql
-- Scans only relevant partitions
SELECT * FROM FactSales 
WHERE SalesDateKey >= '2025-01-01' 
  AND SalesDateKey <= '2025-01-31';

-- Execution: 0.5 seconds (30x faster)
-- Partitions scanned: 31 (January 1-31, 2025)
```

**Verification**:
```sql
-- Check which partitions query will scan
EXPLAIN
SELECT * FROM FactSales 
WHERE SalesDateKey >= '2025-01-01' 
  AND SalesDateKey <= '2025-01-31';

-- Look for partition list in execution plan
```

### Column Selection

**Bad Query** (reads unnecessary columns):
```sql
-- Reads all 14 columns
SELECT * FROM FactSales 
WHERE SalesDateKey = '2025-11-11';

-- Data read: 274K rows × 14 columns = 3.8M values
-- Time: 2 seconds
```

**Good Query** (reads only needed columns):
```sql
-- Reads only 3 columns
SELECT SalesDateKey, ProductKey, SalesRevenue 
FROM FactSales 
WHERE SalesDateKey = '2025-11-11';

-- Data read: 274K rows × 3 columns = 822K values (5x less)
-- Time: 0.4 seconds (5x faster)
```

### Aggregation Optimization

**Bad Query** (aggregates on fact table):
```sql
-- Aggregates 100M rows
SELECT 
    SalesDateKey,
    SUM(SalesRevenue) as total_revenue
FROM FactSales
WHERE SalesDateKey >= '2025-01-01'
GROUP BY SalesDateKey;

-- Time: 5 seconds
```

**Good Query** (uses pre-aggregated table):
```sql
-- Aggregates 365 rows
SELECT 
    SalesDateKey,
    SUM(RevenueSum) as total_revenue
FROM agg_daily_sales
WHERE SalesDateKey >= '2025-01-01'
GROUP BY SalesDateKey;

-- Time: 0.1 seconds (50x faster)
```

### Join Optimization

**Bad Query** (large table first):
```sql
-- FactSales (100M rows) joined to DimDate (3K rows)
SELECT *
FROM FactSales fs
JOIN DimDate d ON fs.SalesDateKey = d.DateKey
WHERE d.Year = 2025;

-- Memory intensive, slower
```

**Good Query** (small table filtered first):
```sql
-- Filter DimDate first (365 rows), then join to FactSales
SELECT *
FROM FactSales fs
JOIN (
    SELECT DateKey FROM DimDate WHERE Year = 2025
) d ON fs.SalesDateKey = d.DateKey;

-- Or use subquery in WHERE:
SELECT *
FROM FactSales
WHERE SalesDateKey IN (
    SELECT DateKey FROM DimDate WHERE Year = 2025
);
```

### SCD Type 2 Join Optimization

**Bad Query** (missing IsCurrent filter):
```sql
-- Joins to ALL customer versions (150K rows)
SELECT 
    fs.SalesRevenue,
    c.CustomerName
FROM FactSales fs
JOIN DimCustomer c ON fs.CustomerKey = c.CustomerKey;

-- Result: Duplicate rows for customers with history
```

**Good Query** (filters to current version):
```sql
-- Joins only to current customer versions (50K rows)
SELECT 
    fs.SalesRevenue,
    c.CustomerName
FROM FactSales fs
JOIN DimCustomer c ON fs.CustomerKey = c.CustomerKey 
    AND c.IsCurrent = 1;

-- 3x fewer rows to scan
```

---

## Partition Management

### Current Partitioning Strategy

**Fact Tables**: Daily partitions by date key
```sql
-- FactSales partitioned by SalesDateKey
PARTITION BY SalesDateKey

-- Creates partitions: 2025-01-01, 2025-01-02, ..., 2025-12-31
```

**Why Daily?**
- Fine-grained partition pruning for date-range queries
- Easy to drop old data by specific dates
- Aligns with daily ETL processing
- Typical partition size: 1-5MB per day

### Partition Lifecycle

**New Partition Creation**:
```sql
-- ClickHouse automatically creates partition on first insert
INSERT INTO FactSales (...) VALUES (...);
-- Creates partition 202512 if doesn't exist
```

**Check Partition Sizes**:
```sql
SELECT 
    partition,
    count() as parts,
    sum(rows) as total_rows,
    formatReadableSize(sum(bytes_on_disk)) as size_on_disk
FROM system.parts
WHERE table = 'FactSales' 
  AND database = 'ADVENTUREWORKS_DWS'
  AND active
GROUP BY partition
ORDER BY partition DESC
LIMIT 10;
```

**Example Output**:
```
partition      | parts | total_rows | size_on_disk
2025-12-31     | 2     | 274,532    | 1.2 MB
2025-12-30     | 2     | 273,890    | 1.1 MB
2025-12-29     | 2     | 275,100    | 1.2 MB
```

### Partition Maintenance

**Drop Old Partitions** (24 month retention):
```sql
-- Drop partitions from 2 years ago (daily partitions)
ALTER TABLE ADVENTUREWORKS_DWS.FactSales 
DROP PARTITION '2023-01-01';

-- Or drop a range of partitions
-- Note: ClickHouse doesn't support DROP PARTITION range directly
-- Use DELETE instead:
ALTER TABLE ADVENTUREWORKS_DWS.FactSales 
DELETE WHERE SalesDateKey < '2023-01-01';

-- Verify deletion
SELECT partition 
FROM system.parts 
WHERE table = 'FactSales' 
  AND partition = '2023-01-01'
  AND active;
-- Should return empty
```

**Optimize Partition** (reduce part count):
```sql
-- When partition has too many parts (>100)
OPTIMIZE TABLE ADVENTUREWORKS_DWS.FactSales 
PARTITION '2025-12-31'
FINAL;

-- Check part count after
SELECT count() as parts 
FROM system.parts
WHERE table = 'FactSales' 
  AND partition = '2025-12-31' 
  AND active;
-- Should be 1 after FINAL optimization
```

### Partition Strategy Evaluation

**Current Strategy (Daily Partitions)**:
- Fine-grained pruning for date-range queries
- Easy to drop old data by specific dates
- Aligns with daily ETL processing
- Typical partition size: 1-5MB per day

**When to use Monthly Partitions**:
- Lower write volume (< 500K rows/day)
- Need larger partitions for better merge efficiency
- Have > 5TB total data

**When to use Yearly Partitions**:
- Very low write volume (< 50K rows/month)
- Rarely query by date range
- Have < 50GB total data

**Current Strategy Validation**:
```sql
-- Check average partition size
SELECT 
    AVG(size_mb) as avg_partition_size_mb,
    MIN(size_mb) as min_size_mb,
    MAX(size_mb) as max_size_mb
FROM (
    SELECT 
        partition,
        sum(bytes_on_disk) / 1024 / 1024 as size_mb
    FROM system.parts
    WHERE table = 'FactSales' AND active
    GROUP BY partition
);

-- Ideal: 1-5 MB per partition (daily)
-- Too small (< 100 KB): Consider monthly partitions
-- Too large (> 50 MB): May need optimization
```

---

## Index Strategy

### Primary Key (ORDER BY)

**Purpose**: Defines sort order on disk, creates sparse primary index.

**Current ORDER BY for FactSales**:
```sql
ORDER BY (SalesDateKey, ProductKey, CustomerKey, StoreKey, SalesOrderID, SalesOrderDetailID)
```

**Why This Order?**:
1. **SalesDateKey first**: Most queries filter by date
2. **ProductKey second**: Product analysis is common
3. **CustomerKey third**: Customer analysis
4. **Grain columns last**: Ensure uniqueness

**Query Performance Impact**:
```sql
-- Fast: Uses primary key prefix
SELECT * FROM FactSales 
WHERE SalesDateKey = '2025-11-11'
  AND ProductKey = 456;
-- Uses index: SalesDateKey, ProductKey

-- Slow: Doesn't use primary key prefix
SELECT * FROM FactSales 
WHERE CustomerKey = 1002;
-- Full table scan (CustomerKey is 3rd in order)
```

### Index Granularity

**Default**: 8192 rows per index mark

```sql
ENGINE = MergeTree()
ORDER BY (SalesDateKey, ProductKey)
SETTINGS index_granularity = 8192;
```

**How It Works**:
- Index stores value every 8,192 rows
- Smaller granularity = more memory, faster lookups
- Larger granularity = less memory, slower lookups

**Adjust for specific tables**:
```sql
-- For large tables with selective queries
SETTINGS index_granularity = 4096;  -- More granular

-- For small lookup tables
SETTINGS index_granularity = 16384; -- Less granular
```

### Secondary Indexes (Skip Indexes)

**Not currently used, but available for optimization**.

**Example: Add skip index for CustomerKey**:
```sql
-- Create skip index
ALTER TABLE ADVENTUREWORKS_DWS.FactSales 
ADD INDEX idx_customer CustomerKey TYPE minmax GRANULARITY 4;

-- Materialize index for existing data
ALTER TABLE ADVENTUREWORKS_DWS.FactSales MATERIALIZE INDEX idx_customer;

-- Now queries on CustomerKey are faster
SELECT * FROM FactSales WHERE CustomerKey = 1002;
-- Uses skip index to eliminate granules
```

**When to use skip indexes**:
- Column frequently queried but not in ORDER BY
- High cardinality columns
- Selective queries (return small % of data)

---

## Capacity Planning

### Current Capacity

**FactSales** (largest table):
- Rows: 120,000,000 (120M)
- Size: 60 GB compressed
- Compression ratio: 10:1
- Growth rate: 274K rows/day (8.2M rows/month)

**Projection** (12 months):
```
Current: 120M rows, 60 GB
Growth: 8.2M rows/month × 12 = 98.4M rows
Projected: 218M rows, 109 GB (after 1 year)
```

### Storage Capacity

**Formula**:
```
Total Storage = (Fact Tables + Dimension Tables + Aggregates) × Safety Factor

FactSales:      60 GB
FactPurchases:  5 GB
FactInventory:  2 GB
Other Facts:    10 GB
Dimensions:     3 GB
Aggregates:     5 GB
-----------------
Subtotal:       85 GB
Safety Factor:  ×1.5
-----------------
Total:          128 GB
```

**Recommendation**: Provision 150 GB minimum, 200 GB comfortable.

### Query Concurrency

**Current**: 5-10 concurrent users

**ClickHouse memory per query**:
```
Average query memory: 500 MB
Peak concurrent queries: 10
Required memory: 10 × 500 MB = 5 GB
```

**Server memory requirement**:
```
Query memory:     5 GB
ClickHouse cache: 10 GB
OS overhead:      3 GB
Buffer:           2 GB
--------------------
Total RAM:        20 GB minimum
Recommended:      32 GB
```

### ETL Processing Time

**Current performance** (hourly load):
- Dimension sync: 5 minutes
- Fact population: 50 minutes
- Total: 55 minutes

**Acceptable**: Under 60 minutes for hourly schedule

**If ETL time approaches 60 minutes**:
1. Optimize queries (partition pruning)
2. Increase batch sizes
3. Parallelize fact loading
4. Add indexes
5. Scale up hardware

**Projection** (1 year):
```
Current: 274K rows/day in 50 minutes
Projected: 274K rows/day (same daily volume)
Expected time: 50 minutes (no degradation expected)

Why? Data is partitioned, only scan current partition
```

### Scaling Triggers

**Vertical Scaling Needed When**:
- Query time > 5 seconds for standard reports
- ETL time > 90 minutes
- CPU usage consistently > 80%
- Memory usage consistently > 80%
- Disk usage > 75%

**Horizontal Scaling Needed When**:
- Data volume > 500 GB
- Concurrent users > 50
- Single server CPU/memory maxed out
- Query complexity increasing

---

## Performance Monitoring

### Key Performance Indicators

**Query Performance**:
```sql
-- Average query duration by user
SELECT 
    user,
    count() as query_count,
    round(avg(query_duration_ms) / 1000, 2) as avg_duration_sec,
    round(max(query_duration_ms) / 1000, 2) as max_duration_sec
FROM system.query_log
WHERE event_date = today()
  AND type = 'QueryFinish'
  AND query NOT LIKE '%system.%'
GROUP BY user
ORDER BY avg_duration_sec DESC;
```

**Insert Performance**:
```sql
-- Insert rate per table
SELECT 
    table,
    count() as insert_count,
    sum(written_rows) as total_rows,
    formatReadableSize(sum(written_bytes)) as total_size,
    round(avg(query_duration_ms) / 1000, 2) as avg_duration_sec
FROM system.query_log
WHERE event_date = today()
  AND type = 'QueryFinish'
  AND query LIKE 'INSERT%'
GROUP BY table
ORDER BY total_rows DESC;
```

**Partition Health**:
```sql
-- Check for partitions with too many parts
SELECT 
    database,
    table,
    partition,
    count() as parts_count
FROM system.parts
WHERE active
GROUP BY database, table, partition
HAVING parts_count > 50
ORDER BY parts_count DESC;

-- Parts > 50: Consider OPTIMIZE TABLE
-- Parts > 100: Optimization needed urgently
```

**Resource Utilization**:
```sql
-- Current resource usage
SELECT 
    metric,
    value
FROM system.asynchronous_metrics
WHERE metric IN (
    'MemoryTracking',
    'DiskAvailable_default',
    'MaxPartCountForPartition',
    'NumberOfDatabases',
    'NumberOfTables'
);
```

### Slow Query Analysis

**Identify slow queries**:
```sql
SELECT 
    query_id,
    user,
    query_duration_ms / 1000 as duration_sec,
    read_rows,
    formatReadableSize(read_bytes) as read_size,
    memory_usage,
    substring(query, 1, 100) as query_preview
FROM system.query_log
WHERE event_date = today()
  AND type = 'QueryFinish'
  AND query_duration_ms > 5000  -- Slower than 5 seconds
ORDER BY query_duration_ms DESC
LIMIT 20;
```

**Explain slow query**:
```sql
-- Get execution plan
EXPLAIN PLAN 
SELECT * FROM FactSales 
WHERE SalesDateKey >= '2025-01-01';

-- Get detailed pipeline
EXPLAIN PIPELINE
SELECT * FROM FactSales 
WHERE SalesDateKey >= '2025-01-01';
```

---

## Optimization Techniques

### Technique 1: Materialized Views for Common Queries

**Problem**: Dashboard query runs every 5 minutes, takes 10 seconds each time.

**Solution**: Create materialized view that updates incrementally.

```sql
-- Create base aggregate view
CREATE MATERIALIZED VIEW mv_hourly_sales_summary
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(SalesHour)
ORDER BY (SalesHour, ProductKey)
AS
SELECT 
    toStartOfHour(SalesDateKey) as SalesHour,
    ProductKey,
    sum(SalesRevenue) as Revenue,
    sum(QuantitySold) as Quantity,
    count() as TransactionCount
FROM FactSales
GROUP BY SalesHour, ProductKey;

-- Now query is instant
SELECT 
    SalesHour,
    sum(Revenue) as total_revenue
FROM mv_hourly_sales_summary
WHERE SalesHour >= now() - INTERVAL 7 DAY
GROUP BY SalesHour
ORDER BY SalesHour DESC;

-- Result: 0.1 seconds instead of 10 seconds
```

### Technique 2: Compression Optimization

**Check current compression**:
```sql
SELECT 
    table,
    formatReadableSize(sum(data_compressed_bytes)) as compressed,
    formatReadableSize(sum(data_uncompressed_bytes)) as uncompressed,
    round(sum(data_uncompressed_bytes) / sum(data_compressed_bytes), 2) as ratio
FROM system.columns
WHERE database = 'ADVENTUREWORKS_DWS'
  AND table = 'FactSales'
GROUP BY table;
```

**Optimize compression** (especially for date/numeric columns):
```sql
-- Add compression codec
ALTER TABLE ADVENTUREWORKS_DWS.FactSales 
MODIFY COLUMN SalesDateKey Date CODEC(Delta, ZSTD);

ALTER TABLE ADVENTUREWORKS_DWS.FactSales 
MODIFY COLUMN SalesRevenue Decimal(18,2) CODEC(Gorilla, ZSTD);

-- Explanation:
-- Delta: Stores differences (good for sequential dates)
-- Gorilla: Optimized for floating point (timestamps, prices)
-- ZSTD: General compression algorithm
-- Combined: Can achieve 20:1 compression ratios
```

### Technique 3: Sample Queries for Development

**Problem**: Development queries scan full production data (slow, expensive).

**Solution**: Use SAMPLE clause for fast approximations.

```sql
-- Sample 10% of data for development/testing
SELECT 
    ProductKey,
    AVG(SalesRevenue) as avg_revenue
FROM FactSales SAMPLE 0.1
WHERE SalesDateKey >= '2025-01-01'
GROUP BY ProductKey;

-- 10x faster, still representative results
```

### Technique 4: Query Result Caching

**Enable query cache** (in ClickHouse config):
```xml
<clickhouse>
    <query_cache>
        <max_size_in_bytes>5368709120</max_size_in_bytes>
        <max_entries>1000</max_entries>
        <max_entry_size_in_bytes>10485760</max_entry_size_in_bytes>
    </query_cache>
</clickhouse>
```

**Use cache in queries**:
```sql
-- Cache dashboard query results
SELECT 
    SalesDateKey,
    SUM(SalesRevenue) as revenue
FROM FactSales
WHERE SalesDateKey >= today() - 30
GROUP BY SalesDateKey
SETTINGS use_query_cache = 1;

-- Second execution: Instant (from cache)
```

### Technique 5: Pre-calculate Complex Metrics

**Problem**: Complex calculation runs on every query.

```sql
-- Slow: Calculate profit margin on every query
SELECT 
    ProductKey,
    SUM(SalesRevenue - (QuantitySold * p.Cost)) as profit,
    SUM(SalesRevenue - (QuantitySold * p.Cost)) / SUM(SalesRevenue) * 100 as margin
FROM FactSales fs
JOIN DimProduct p ON fs.ProductKey = p.ProductKey
GROUP BY ProductKey;
```

**Solution**: Add calculated column to fact table during ETL.

```sql
-- Add column to FactSales
ALTER TABLE ADVENTUREWORKS_DWS.FactSales 
ADD COLUMN ProfitAmount Decimal(18,2);

-- In Python DAG:
# profit_amount = sales_revenue - (quantity * cost)

-- Fast query: Pre-calculated profit
SELECT 
    ProductKey,
    SUM(ProfitAmount) as profit,
    SUM(ProfitAmount) / SUM(SalesRevenue) * 100 as margin
FROM FactSales
GROUP BY ProductKey;

-- 5x faster (no join, no calculation)
```

---

## Summary

**Top 5 Performance Optimizations**:
1. **Partition pruning**: Always filter on date in WHERE clause
2. **Column selection**: Select only needed columns, not SELECT *
3. **Pre-aggregated tables**: Use for common dashboard queries
4. **SCD Type 2 joins**: Always include IsCurrent = 1 filter
5. **Regular OPTIMIZE**: Run weekly on active partitions

**Capacity Planning Guidelines**:
- Storage: 150 GB minimum, scale at 75% usage
- Memory: 32 GB recommended for 10 concurrent users
- CPU: 8 cores minimum
- ETL window: Keep under 60 minutes for hourly loads

**Monitoring Checklist**:
- Query duration trends
- Partition sizes
- Part counts per partition
- Error record growth
- Resource utilization

**Next Steps**:
- See [Deployment Runbook](deployment_runbook.md) for optimization procedures
- See [ClickHouse Physical Design](clickhouse_physical_design.md) for schema optimization
- See [FAQ & Troubleshooting](faq_troubleshooting.md) for performance issues