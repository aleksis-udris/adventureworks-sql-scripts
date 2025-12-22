# Airflow DAG Specification

## Table of Contents
1. [DAG Overview](#dag-overview)
2. [Naming Convention](#naming-convention)
3. [High-Level DAG Structure](#high-level-dag-structure)
4. [Task Specifications](#task-specifications)
5. [Scheduling Parameters](#scheduling-parameters)
6. [XCom Usage](#xcom-usage)
7. [Error Handling](#error-handling)

---

## DAG Overview

### Purpose

Apache Airflow orchestrates the entire ETL pipeline from PostgreSQL source to ClickHouse warehouse.

**Key DAGs**:
- `adventureworks_dimension_sync` - Load/update dimension tables (hourly)
- `adventureworks_fact_population` - Load fact tables (hourly)
- `dag_dwh_error_recovery` - Reprocess recoverable errors (daily)

---

## Naming Convention

```
dag_<project>_<frequency>_v<version>

Examples:
- adventureworks_dimension_sync    (dimension loading)
- adventureworks_fact_population   (fact loading)
- dag_dwh_error_recovery_v1        (error reprocessing)
```

**Task Naming**:
```
<action>_<object>

Examples:
- extract_incremental_data
- validate_extracted_data
- load_dim_customer_scd2
- load_fact_sales
- update_aggregates
```

---

## High-Level DAG Structure

### Fact Population DAG: `adventureworks_fact_population`

```
start
  ↓
[extract_resolve_and_load_factsales]
  (PostgreSQL → ClickHouse, 50 min)
  Output: 274K rows inserted
  ↓
[extract_resolve_and_load_factpurchases]
  (Parallel with other facts)
  ↓
[extract_resolve_and_load_factinventory]
  (Parallel with other facts)
  ↓
success (DAG completed)
```

### Dimension Sync DAG: `adventureworks_dimension_sync`

```
start
  ↓
[extract_postgres_dimension_dimcustomer] ──┐
[extract_clickhouse_dimension_dimcustomer] ─┤
                                            ↓
                            [sync_dimension_table_dimcustomer]
                                            ↓
[extract_postgres_dimension_dimproduct] ───┐
[extract_clickhouse_dimension_dimproduct] ──┤
                                            ↓
                            [sync_dimension_table_dimproduct]
                                            ↓
                                        success
```

---

## Task Specifications

### Task: extract_resolve_and_load (Fact Tables)

**Purpose**: Extract from PostgreSQL, resolve FKs, load to ClickHouse.

```yaml
Task ID: extract_resolve_and_load_factsales
Operator: PythonOperator
Function: extract_resolve_and_load(fact_table='FactSales')

Input:
  - PostgreSQL connection string
  - Fact table configuration (query, columns, FK specs)
  - Batch ID: "batch_20251222_020000"

Logic:
  1. Connect to PostgreSQL with server-side cursor
  2. Fetch rows in batches (50K at a time)
  3. Clean data (handle nulls, convert types)
  4. Resolve foreign keys:
     a. Bulk lookup CustomerKey from DimCustomer (WHERE IsCurrent=1)
     b. Bulk lookup ProductKey from DimProduct (WHERE IsCurrent=1)
     c. If FK missing:
        - Log error to error_records
        - Skip row (recoverable error)
  5. Insert valid rows to ClickHouse (10K per batch)
  6. Return: {"inserted": 273890, "errors": 642}

Output:
  - ClickHouse table: FactSales (273,890 rows)
  - error_records: 642 recoverable errors (FK miss)

Expected Runtime: 50 minutes
Retry: 3 attempts with 5-minute delay
Timeout: 2 hours
```

**Code Example**:
```python
@task
def extract_resolve_and_load(fact_table: str) -> int:
    cfg = FACTS[fact_table]
    query = cfg["query"]
    insert_columns = cfg["insert_columns"]
    fk_specs = cfg.get("fk_specs", [])
    
    batch_id = f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    ch_client = ch()
    
    inserted_total = 0
    error_total = 0
    
    # Server-side cursor for large datasets
    conn = pg()
    cur = conn.cursor(name=f"{fact_table.lower()}_cursor")
    cur.itersize = 50000  # Fetch 50K rows at a time
    
    cur.execute(query)
    
    while True:
        rows = cur.fetchmany(50000)
        if not rows:
            break
        
        # Resolve FKs and build valid rows
        valid_rows, error_rows = _resolve_and_build_rows(
            fact_table=fact_table,
            cleaned_rows=rows,
            fk_specs=fk_specs,
            ch_client=ch_client,
            batch_id=batch_id
        )
        
        # Insert valid rows in 10K batches
        for batch in chunks(valid_rows, 10000):
            ch_client.insert(
                f"ADVENTUREWORKS_DWS.{fact_table}",
                batch,
                column_names=insert_columns
            )
            inserted_total += len(batch)
        
        # Log errors
        if error_rows:
            insert_error_records(ch_client, error_rows)
            error_total += len(error_rows)
    
    print(f"[{fact_table}] Inserted={inserted_total} Errors={error_total}")
    return inserted_total
```

---

### Task: sync_dimension_table (Dimensions)

**Purpose**: Synchronize dimension table between PostgreSQL and ClickHouse with SCD Type 2 logic.

```yaml
Task ID: sync_dimension_table_dimcustomer
Operator: PythonOperator
Function: sync_dimension_table(table='DimCustomer', ...)

Input:
  - PostgreSQL dimension data (pickled file)
  - ClickHouse dimension data (pickled file)
  - Natural key: "CustomerID"
  - SCD Type 2 enabled

Logic:
  1. Load data from pickle files
  2. Build lookup maps: {natural_key -> row}
  3. Detect changes:
     a. New records: in PG but not in CH → INSERT
     b. Deleted records: in CH but not in PG → UPDATE IsCurrent=0
     c. Changed records: compare attributes → INSERT new version
  4. For changed records (SCD Type 2):
     a. Expire old version: UPDATE IsCurrent=0, ValidToDate=TODAY-1
     b. Insert new version: IsCurrent=1, ValidFromDate=TODAY
  5. OPTIMIZE TABLE (merge parts)

Output:
  - DimCustomer: 25 new versions inserted
  - DimCustomer: 12 old versions expired

Expected Runtime: 2-5 minutes
Retry: 3 attempts
```

**SCD Type 2 Logic**:
```python
# Detect change
if pg_row.email != ch_row.email:
    # Expire old version
    client.command(
        f"ALTER TABLE DimCustomer "
        f"UPDATE IsCurrent = 0, ValidToDate = CURRENT_DATE - 1 "
        f"WHERE CustomerID = {customer_id} AND IsCurrent = 1"
    )
    
    # Insert new version
    pg_row.Version = ch_row.Version + 1
    pg_row.IsCurrent = 1
    pg_row.ValidFromDate = CURRENT_DATE
    pg_row.ValidToDate = None
    client.insert("DimCustomer", [pg_row])
```

---

## Scheduling Parameters

### Main ETL DAG Configuration

```python
from airflow import DAG
from datetime import datetime, timedelta

default_args = {
    'owner': 'data-engineering',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(hours=1),
    'email': ['data-warehouse@company.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'start_date': datetime(2023, 1, 1),
    'depends_on_past': False,  # Don't wait for previous run
    'execution_timeout': timedelta(hours=3)
}

dag = DAG(
    'adventureworks_fact_population',
    default_args=default_args,
    schedule='@hourly',  # Run every hour
    description='AdventureWorks fact table incremental load',
    catchup=False,  # Don't backfill missed runs
    max_active_runs=1,  # Only one execution at a time
    tags=['adventureworks', 'facts', 'etl']
)
```

**Schedule Options**:
```python
schedule='@hourly'   # Every hour: 00:00, 01:00, 02:00, ...
schedule='@daily'    # Daily at midnight
schedule='0 2 * * *' # Daily at 02:00 UTC
schedule='*/30 * * * *' # Every 30 minutes
```

---

## XCom Usage

### Passing Data Between Tasks

**XCom** (cross-communication) lets tasks share small amounts of data.

**Example 1: Row Count Validation**

```python
# Task A: Extract (push row count)
@task
def extract_incremental_data():
    rows_loaded = 274532
    return rows_loaded  # Automatically pushed to XCom

# Task B: Validate (pull row count)
@task
def validate_data_quality(rows_loaded: int):
    if rows_loaded < 100000:
        raise ValueError(f"Too few rows: {rows_loaded}")
    elif rows_loaded > 1000000:
        raise ValueError(f"Too many rows: {rows_loaded}")
    
    return "Validation passed"

# Wire tasks
row_count = extract_incremental_data()
validate_data_quality(row_count)
```

**Example 2: Manual XCom Push/Pull**

```python
@task
def extract_data(**context):
    rows_loaded = 274532
    
    # Manual push
    ti = context['task_instance']
    ti.xcom_push(key='rows_loaded', value=rows_loaded)
    ti.xcom_push(key='max_timestamp', value='2025-11-11 23:59:59')
    
    return rows_loaded

@task
def validate_data(**context):
    # Manual pull
    ti = context['task_instance']
    rows_loaded = ti.xcom_pull(
        task_ids='extract_data',
        key='rows_loaded'
    )
    max_timestamp = ti.xcom_pull(
        task_ids='extract_data',
        key='max_timestamp'
    )
    
    print(f"Validating {rows_loaded} rows up to {max_timestamp}")
```

**XCom Limitations**:
- Max size: 48KB (use files for larger data)
- Stored in Airflow metadata DB
- Not for large datasets (use pickle files instead)

---

## Error Handling

### Retry Strategy

**Exponential Backoff**:
```python
default_args = {
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(hours=1)
}

# Retry timeline:
# Attempt 1: 02:00 (fails)
# Attempt 2: 02:05 (5 min delay, fails)
# Attempt 3: 02:15 (10 min delay, fails)
# Attempt 4: 02:35 (20 min delay)
```

### Task-Level Error Handling

```python
@task
def load_fact_sales():
    try:
        # Main logic
        rows = extract_from_postgres()
        insert_to_clickhouse(rows)
        return {"status": "success", "rows": len(rows)}
    
    except ClickHouseException as e:
        # Database error - retryable
        log_error(e)
        raise  # Retry this task
    
    except ValueError as e:
        # Data validation error - skip bad data
        log_error(e)
        return {"status": "partial", "errors": str(e)}
    
    except Exception as e:
        # Unexpected error - fail and alert
        send_alert(e)
        raise
```

### Callback Functions

```python
def on_failure_callback(context):
    """Called when task fails after all retries"""
    task_instance = context['task_instance']
    exception = context.get('exception')
    
    send_email(
        to=['data-warehouse@company.com'],
        subject=f"Task Failed: {task_instance.task_id}",
        html_content=f"""
        Task Failure Alert
        DAG: {task_instance.dag_id}
        Task: {task_instance.task_id}
        Error: {exception}
        Log: View Logs
        """
    )

@task(on_failure_callback=on_failure_callback)
def critical_task():
    # This task triggers email on failure
    pass
```

---

## DAG Execution Flow

### Visual Timeline

```
02:00:00 - DAG triggered (schedule: @hourly)
02:00:05 - extract_resolve_and_load_factsales (starts)
02:50:15 - extract_resolve_and_load_factsales (completes)
02:50:20 - extract_resolve_and_load_factpurchases (starts)
02:55:30 - extract_resolve_and_load_factpurchases (completes)
02:55:35 - DAG success

Total: 55 minutes 35 seconds
```

### Monitoring Metrics

**Airflow UI shows**:
- Task duration: 50m 10s (normal: 45-55 min)
- Rows processed: 274,532
- Errors: 642 (recoverable)
- Success rate: 99.77%
- Next run: 03:00:00

---

## Summary

**Key Components**:
1. **DAG naming**: `<project>_<purpose>` pattern
2. **Task naming**: `<action>_<object>` pattern
3. **Scheduling**: Hourly for facts, daily for errors
4. **XCom**: Share small data between tasks
5. **Retry logic**: Exponential backoff (5→10→20 min)
6. **Error handling**: Task-level try/catch + callbacks

**Best Practices**:
- Use `@task` decorator for concise code
- Set `max_active_runs=1` to prevent overlaps
- Use `catchup=False` unless backfilling needed
- Log errors to error_records table
- Monitor task duration for anomalies

**Next Steps**:
- See [Transformation Logic](transformation_logic.md) for SCD Type 2 details
- See [Error Handling & Monitoring](error_handling.md) for error recovery
- See [Deployment Runbook](deployment_runbook.md) for production setup