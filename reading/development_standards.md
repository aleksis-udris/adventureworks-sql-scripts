# Development Standards and Best Practices

## Table of Contents
1. [Naming Conventions](#naming-conventions)
2. [SQL Style Guide](#sql-style-guide)
3. [Python Code Standards](#python-code-standards)
4. [DAG Development](#dag-development)
5. [Testing Practices](#testing-practices)
6. [Documentation Requirements](#documentation-requirements)

---

## Naming Conventions

### Table Names

**Fact Tables**:
```
Pattern: Fact<EntityName>
Examples: FactSales, FactPurchases, FactInventory
```

**Dimension Tables**:
```
Pattern: Dim<EntityName>
Examples: DimCustomer, DimProduct, DimDate
```

**Aggregate Tables**:
```
Pattern: agg_<frequency>_<subject>
Examples: agg_daily_sales, agg_weekly_sales, agg_monthly_product_performance
```

**Staging Tables**:
```
Pattern: stg_<source>_<entity>
Examples: stg_orders, stg_customers, stg_products
```

**Error Tables**:
```
Pattern: error_<purpose>
Example: error_records
```

### Column Names

**Keys**:
```
Surrogate keys: <TableName>Key (CustomerKey, ProductKey)
Natural keys: <TableName>ID (CustomerID, ProductID)
Foreign keys: <ReferencedTable>Key (CustomerKey in FactSales)
```

**Measures**:
```
Pattern: <MeasureName> with clear units
Examples: SalesRevenue, QuantitySold, DiscountAmount
Not: Amount, Qty, Discount (ambiguous)
```

**Attributes**:
```
Pattern: <AttributeName> without abbreviations
Examples: CustomerName, EmailAddress, PhoneNumber
Not: CustName, Email, Phone
```

**SCD Columns**:
```
Required for SCD Type 2:
- ValidFromDate
- ValidToDate
- IsCurrent
- Version
- SourceUpdateDate
```

### Variable Names

**Python**:
```python
# Snake case for variables and functions
customer_key = 1001
sales_revenue = 450.50

def calculate_total_revenue(fact_rows):
    return sum(row['SalesRevenue'] for row in fact_rows)
```

**SQL**:
```sql
-- Pascal case for tables
SELECT * FROM FactSales

-- camelCase or snake_case for aliases (be consistent)
SELECT fs.SalesRevenue as sales_revenue
FROM FactSales fs
```

---

## SQL Style Guide

### Query Formatting

**SELECT statements**:

```sql
-- Good: Formatted, readable
SELECT 
    fs.SalesDateKey,
    c.CustomerName,
    p.ProductName,
    SUM(fs.SalesRevenue) as total_revenue,
    COUNT(*) as transaction_count
FROM FactSales fs
JOIN DimCustomer c ON fs.CustomerKey = c.CustomerKey AND c.IsCurrent = 1
JOIN DimProduct p ON fs.ProductKey = p.ProductKey AND p.IsCurrent = 1
WHERE fs.SalesDateKey >= '2025-01-01'
GROUP BY 
    fs.SalesDateKey,
    c.CustomerName,
    p.ProductName
ORDER BY total_revenue DESC
LIMIT 10;

-- Bad: Single line, unreadable
SELECT fs.SalesDateKey, c.CustomerName, p.ProductName, SUM(fs.SalesRevenue) as total_revenue, COUNT(*) as transaction_count FROM FactSales fs JOIN DimCustomer c ON fs.CustomerKey = c.CustomerKey AND c.IsCurrent = 1 JOIN DimProduct p ON fs.ProductKey = p.ProductKey AND p.IsCurrent = 1 WHERE fs.SalesDateKey >= '2025-01-01' GROUP BY fs.SalesDateKey, c.CustomerName, p.ProductName ORDER BY total_revenue DESC LIMIT 10;
```

**JOIN conditions**:

```sql
-- Good: Explicit JOIN with ON
SELECT *
FROM FactSales fs
JOIN DimCustomer c ON fs.CustomerKey = c.CustomerKey

-- Bad: Implicit join in WHERE
SELECT *
FROM FactSales fs, DimCustomer c
WHERE fs.CustomerKey = c.CustomerKey
```

**WHERE clauses**:

```sql
-- Good: One condition per line for complex filters
WHERE fs.SalesDateKey >= '2025-01-01'
  AND fs.SalesRevenue > 0
  AND c.IsCurrent = 1
  AND p.ProductStatus = 'Active'

-- Bad: All on one line
WHERE fs.SalesDateKey >= '2025-01-01' AND fs.SalesRevenue > 0 AND c.IsCurrent = 1 AND p.ProductStatus = 'Active'
```

### DDL Standards

**CREATE TABLE**:

```sql
CREATE TABLE IF NOT EXISTS ADVENTUREWORKS_DWS.FactSales (
    -- Foreign Keys
    SalesDateKey Date,
    CustomerKey UInt64,
    ProductKey UInt64,
    
    -- Grain
    SalesOrderID UInt32,
    SalesOrderDetailID UInt32,
    
    -- Measures
    QuantitySold UInt32,
    SalesRevenue Decimal(18, 2),
    DiscountAmount Decimal(18, 2),
    
    -- Metadata
    InsertedAt DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(SalesDateKey)
ORDER BY (SalesDateKey, ProductKey, CustomerKey, StoreKey);
```

Structure:
1. Foreign keys first
2. Grain identifiers
3. Measures
4. Metadata columns last
5. Comments for complex logic

### Commenting

```sql
-- Calculate total revenue by product category
-- Excludes returns and cancelled orders
-- Only includes data from last 30 days
SELECT 
    p.Category,
    SUM(fs.SalesRevenue) as total_revenue  -- Revenue before discounts
FROM FactSales fs
JOIN DimProduct p ON fs.ProductKey = p.ProductKey
WHERE fs.SalesDateKey >= today() - 30  -- Last 30 days only
  AND fs.SalesRevenue > 0              -- Exclude returns (negative amounts)
GROUP BY p.Category;
```

Use comments for:
- Query purpose
- Business logic explanation
- Non-obvious filters
- Complex calculations

---

## Python Code Standards

### Function Structure

```python
def load_dimension_scd2(
    pg_rows: List[Dict],
    ch_rows: List[Dict],
    table_name: str,
    natural_key: str,
    tracked_columns: List[str]
) -> Dict[str, int]:
    """
    Apply SCD Type 2 logic to dimension table.
    
    Args:
        pg_rows: Rows from PostgreSQL source
        ch_rows: Rows from ClickHouse warehouse
        table_name: Name of dimension table
        natural_key: Column name of natural key
        tracked_columns: Columns to monitor for changes
    
    Returns:
        Dictionary with counts: {'inserted': 10, 'updated': 5, 'expired': 3}
    
    Example:
        result = load_dimension_scd2(
            pg_rows=source_customers,
            ch_rows=warehouse_customers,
            table_name='DimCustomer',
            natural_key='CustomerID',
            tracked_columns=['Email', 'City', 'Country']
        )
    """
    # Implementation
    pass
```

Always include:
- Type hints for parameters and return
- Docstring with purpose, args, returns, example
- Clear variable names
- Comments for complex logic

### Error Handling

```python
def extract_from_source(query: str, connection_string: str) -> List[Dict]:
    """Extract data from source database."""
    try:
        conn = psycopg2.connect(connection_string)
        cur = conn.cursor()
        cur.execute(query)
        rows = cur.fetchall()
        return rows
        
    except psycopg2.OperationalError as e:
        # Connection issues - log and raise
        logger.error(f"Cannot connect to database: {e}")
        raise
        
    except psycopg2.ProgrammingError as e:
        # SQL syntax error - log and raise
        logger.error(f"Invalid SQL query: {e}")
        raise
        
    except Exception as e:
        # Unexpected error - log and raise
        logger.error(f"Unexpected error during extraction: {e}")
        raise
        
    finally:
        # Always cleanup
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()
```

### Logging

```python
import logging

logger = logging.getLogger(__name__)

def process_batch(rows: List[Dict], batch_id: str):
    """Process batch of rows."""
    logger.info(f"Starting batch {batch_id} with {len(rows)} rows")
    
    try:
        valid_rows, error_rows = validate_rows(rows)
        logger.info(f"Validated: {len(valid_rows)} valid, {len(error_rows)} errors")
        
        insert_to_warehouse(valid_rows)
        logger.info(f"Inserted {len(valid_rows)} rows successfully")
        
        if error_rows:
            log_errors(error_rows)
            logger.warning(f"Logged {len(error_rows)} errors to error_records")
        
    except Exception as e:
        logger.error(f"Batch {batch_id} failed: {e}", exc_info=True)
        raise
```

Log levels:
- DEBUG: Detailed diagnostic information
- INFO: General informational messages
- WARNING: Something unexpected but recoverable
- ERROR: Error that prevented operation
- CRITICAL: Serious error requiring immediate attention

---

## DAG Development

### DAG Structure

```python
from airflow.sdk import dag, task
from datetime import datetime, timedelta

@dag(
    dag_id="adventureworks_fact_population",
    dag_display_name="Populate Fact Tables",
    schedule="@hourly",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["adventureworks", "facts", "etl"],
    default_args={
        'owner': 'data-engineering',
        'retries': 3,
        'retry_delay': timedelta(minutes=5),
        'email': ['alerts@company.com'],
        'email_on_failure': True
    }
)
def populate_facts():
    """
    Load fact tables from PostgreSQL to ClickHouse.
    
    Schedule: Hourly
    Duration: ~50 minutes
    Dependencies: Dimensions must be loaded first
    """
    
    @task
    def extract_and_load(table_name: str) -> int:
        """Extract from source and load to warehouse."""
        # Implementation
        pass
    
    # Define task dependencies
    for table in ['FactSales', 'FactPurchases', 'FactInventory']:
        extract_and_load(table)

populate_facts()
```

### Task Naming

```python
# Good: Descriptive task IDs
@task(task_id="extract_sales_from_postgres")
def extract_sales():
    pass

@task(task_id="transform_customer_dimensions")
def transform_customers():
    pass

@task(task_id="load_facts_to_clickhouse")
def load_facts():
    pass

# Bad: Vague task IDs
@task(task_id="task1")
def do_stuff():
    pass
```

### Dependencies

```python
# Good: Clear dependency chain
extract = extract_data()
validate = validate_data(extract)
load = load_to_warehouse(validate)

# Good: Parallel tasks
dim_customer = load_dimension('DimCustomer')
dim_product = load_dimension('DimProduct')
facts = load_facts()

dim_customer >> facts
dim_product >> facts

# Bad: Unclear dependencies
task1 = func1()
task2 = func2()
task3 = func3()
```

---

## Testing Practices

### Unit Tests

```python
import unittest
from etl.transformations import apply_scd_type_2

class TestSCDType2(unittest.TestCase):
    """Test SCD Type 2 transformation logic."""
    
    def test_new_record_insertion(self):
        """Test that new records are inserted with IsCurrent=1."""
        pg_rows = [{'CustomerID': 500, 'Email': 'new@example.com'}]
        ch_rows = []
        
        result = apply_scd_type_2(pg_rows, ch_rows, 'DimCustomer', 'CustomerID')
        
        self.assertEqual(len(result['to_insert']), 1)
        self.assertEqual(result['to_insert'][0]['IsCurrent'], 1)
    
    def test_unchanged_record_skipped(self):
        """Test that unchanged records are not processed."""
        pg_rows = [{'CustomerID': 500, 'Email': 'same@example.com'}]
        ch_rows = [{'CustomerID': 500, 'Email': 'same@example.com', 'IsCurrent': 1}]
        
        result = apply_scd_type_2(pg_rows, ch_rows, 'DimCustomer', 'CustomerID')
        
        self.assertEqual(len(result['to_insert']), 0)
        self.assertEqual(len(result['to_expire']), 0)
    
    def test_changed_record_creates_version(self):
        """Test that changed records create new version and expire old."""
        pg_rows = [{'CustomerID': 500, 'Email': 'new@example.com'}]
        ch_rows = [{'CustomerID': 500, 'Email': 'old@example.com', 'IsCurrent': 1}]
        
        result = apply_scd_type_2(pg_rows, ch_rows, 'DimCustomer', 'CustomerID')
        
        self.assertEqual(len(result['to_insert']), 1)
        self.assertEqual(len(result['to_expire']), 1)
        self.assertEqual(result['to_insert'][0]['Email'], 'new@example.com')
```

### Integration Tests

```python
def test_full_etl_flow():
    """Test complete ETL flow from source to warehouse."""
    # Setup
    load_test_data_to_postgres()
    truncate_clickhouse_tables()
    
    # Execute
    trigger_dag('adventureworks_dimension_sync')
    wait_for_completion()
    trigger_dag('adventureworks_fact_population')
    wait_for_completion()
    
    # Verify
    ch_client = clickhouse_connect.get_client()
    
    # Check row counts
    dim_count = ch_client.query("SELECT COUNT(*) FROM DimCustomer").first_row[0]
    assert dim_count > 0, "Dimensions not loaded"
    
    fact_count = ch_client.query("SELECT COUNT(*) FROM FactSales").first_row[0]
    assert fact_count > 0, "Facts not loaded"
    
    # Check data quality
    null_fk = ch_client.query(
        "SELECT COUNT(*) FROM FactSales WHERE CustomerKey = 0"
    ).first_row[0]
    assert null_fk == 0, "Found NULL foreign keys"
```

### Manual Testing Checklist

Before deploying:

- [ ] Test DAG in Airflow UI (trigger manually)
- [ ] Verify row counts match expectations
- [ ] Check error_records table for errors
- [ ] Run sample analytical queries
- [ ] Verify SCD Type 2 history correct
- [ ] Check query performance (under SLA)
- [ ] Test rollback procedure
- [ ] Verify alerts configured

---

## Documentation Requirements

### Code Documentation

Every DAG file must include:

```python
"""
AdventureWorks Fact Population DAG

Purpose: 
    Load fact tables from PostgreSQL to ClickHouse warehouse.

Schedule: 
    Hourly (@hourly)

Dependencies: 
    - Dimension tables must be loaded first
    - PostgreSQL source database accessible
    - ClickHouse warehouse accessible

Runtime:
    Typical: 50 minutes
    Maximum: 2 hours (timeout)

Alerts:
    - Email on failure
    - Slack notification on retry
    - PagerDuty if exceeds 2 hour timeout

Author: Data Engineering Team
Last Modified: 2025-12-22
"""
```

### Function Documentation

```python
def resolve_foreign_keys(
    source_rows: List[Dict],
    dimension_table: str,
    natural_key: str,
    surrogate_key: str
) -> Tuple[List[Dict], List[Dict]]:
    """
    Resolve natural keys to surrogate keys from dimension table.
    
    This function performs bulk lookup of surrogate keys for fact table loading.
    Uses ClickHouse to query current dimension versions (IsCurrent=1) and
    returns both successfully resolved rows and error rows for logging.
    
    Args:
        source_rows: List of fact rows with natural keys
        dimension_table: Name of dimension table (e.g., 'DimCustomer')
        natural_key: Column name of natural key (e.g., 'CustomerID')
        surrogate_key: Column name of surrogate key (e.g., 'CustomerKey')
    
    Returns:
        Tuple of (valid_rows, error_rows)
        - valid_rows: Rows with resolved surrogate keys
        - error_rows: Rows where FK resolution failed
    
    Example:
        valid, errors = resolve_foreign_keys(
            source_rows=[{'CustomerID': 500, 'Amount': 100}],
            dimension_table='DimCustomer',
            natural_key='CustomerID',
            surrogate_key='CustomerKey'
        )
        
        # valid = [{'CustomerKey': 1002, 'Amount': 100}]
        # errors = []
    
    Raises:
        ConnectionError: If cannot connect to ClickHouse
        ValueError: If dimension_table not found
    """
    # Implementation
    pass
```

### README Files

Every component directory needs README.md:

```markdown
# Dimension ETL

Python scripts for loading dimension tables from PostgreSQL to ClickHouse.

## Functions

- `dimension_loader`: Main ETL logic
- `scd_handler`: SCD Type 2 transformation
- `validation`: Data quality checks

## Usage

python dimension_etl.py --table DimCustomer --date 2025-12-22

## Dependencies

- psycopg2: PostgreSQL connection
- clickhouse-connect: ClickHouse connection
- python-dotenv: Environment variables

## Testing

pytest tests/test_dimension_loader.py
```

---

## Summary

Key standards:
- Consistent naming conventions across all objects
- Formatted SQL for readability
- Type-hinted Python functions with docstrings
- Comprehensive error handling and logging
- Unit and integration tests required
- Documentation for all DAGs and functions

Before committing code:
1. Run linter (pylint, flake8)
2. Run tests (pytest)
3. Update documentation
4. Test manually in development
5. Peer review
6. Deploy to staging first