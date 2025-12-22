# Error Handling & Monitoring

## Table of Contents
1. [Error Records Table](#error-records-table)
2. [Error Classification](#error-classification)
3. [Error Handling in Code](#error-handling-in-code)
4. [Retry Strategy](#retry-strategy)
5. [Error Reprocessing](#error-reprocessing)
6. [Alerting Strategy](#alerting-strategy)
7. [Monitoring Dashboard](#monitoring-dashboard)

---

## Error Records Table

### DDL & Semantics

**Purpose**: Centralized error tracking for all ETL failures.

```sql
CREATE TABLE ADVENTUREWORKS_DWS.error_records (
    ErrorID UInt64,
    ErrorDate DateTime,
    SourceTable String,
    RecordNaturalKey String,
    ErrorType String,
    ErrorSeverity String,
    ErrorMessage String,
    FailedData String,
    ProcessingBatchID String,
    TaskName String,
    IsRecoverable UInt8,
    RetryCount UInt8 DEFAULT 0,
    LastAttemptDate Nullable(DateTime),
    IsResolved UInt8 DEFAULT 0,
    ResolutionComment Nullable(String)
) ENGINE = MergeTree()
ORDER BY (ErrorDate, SourceTable, ErrorType)
PARTITION BY toYYYYMM(ErrorDate);
```

### Column Semantics

| Column            | Type     | Purpose                   | Example                                    |
|-------------------|----------|---------------------------|--------------------------------------------|
| ErrorID           | UInt64   | Unique identifier         | 1001                                       |
| ErrorDate         | DateTime | When error occurred       | 2025-11-11 02:30:00                        |
| SourceTable       | String   | Which table had issue     | FactSales, DimCustomer                     |
| RecordNaturalKey  | String   | Which record failed       | order-12345-line-3                         |
| ErrorType         | String   | Category                  | FK_MISS, NULL_PK, TYPE_MISMATCH            |
| ErrorSeverity     | String   | Impact level              | Critical, Warning, Info                    |
| ErrorMessage      | String   | Brief description         | "CustomerKey not found for CustomerID=999" |
| FailedData        | String   | JSON of failed row        | {"order_id": 12345, "customer_id": 999}    |
| ProcessingBatchID | String   | Batch ID for grouping     | batch-2025-11-11-001                       |
| TaskName          | String   | Which Airflow task        | load_fact_sales                            |
| IsRecoverable     | UInt8    | Can retry?                | 1=yes, 0=no                                |
| RetryCount        | UInt8    | Times retried             | 0, 1, 2, 3                                 |
| LastAttemptDate   | DateTime | When last retry attempted | 2025-11-11 03:00:00                        |
| IsResolved        | UInt8    | Fixed?                    | 1=yes, 0=no                                |
| ResolutionComment | String   | How was it fixed          | "Customer added manually"                  |

### Example Error Record

```json
{
  "ErrorID": 1001,
  "ErrorDate": "2025-11-11 02:30:00",
  "SourceTable": "FactSales",
  "RecordNaturalKey": "order-12345-line-3",
  "ErrorType": "FK_MISS",
  "ErrorSeverity": "WARNING",
  "ErrorMessage": "CustomerKey not found for CustomerID=999",
  "FailedData": "{\"order_id\": 12345, \"customer_id\": 999, \"amount\": 450.50, \"qty\": 2}",
  "ProcessingBatchID": "batch-2025-11-11-001",
  "TaskName": "load_fact_sales",
  "IsRecoverable": 1,
  "RetryCount": 0,
  "LastAttemptDate": null,
  "IsResolved": 0,
  "ResolutionComment": null
}
```

---

## Error Classification

### Complete Error Taxonomy

| Error Type            | Recoverable | Example Scenario                   | Handling                      |
|-----------------------|-------------|------------------------------------|-------------------------------|
| **FK_MISS**           | Yes         | Customer ID 999 not in DimCustomer | Skip row, log, retry tomorrow |
| **NULL_PK**           | No          | OrderID is NULL in source          | Stop DAG, escalate            |
| **TYPE_MISMATCH**     | Sometimes   | Amount = "ABC" (text not decimal)  | Try conversion, log if fail   |
| **DUPLICATE_PK**      | Yes         | OrderID=12345 loaded twice today   | Skip duplicate, log           |
| **OUT_OF_RANGE**      | Yes         | Amount = -999999 (negative)        | Skip row, log                 |
| **VALIDATION_FAIL**   | Yes         | Price < Cost (business rule)       | Skip row, log                 |
| **SCHEMA_MISMATCH**   | No          | Column OrderDate missing           | Stop DAG, manual fix          |
| **TIMEOUT**           | Yes         | PostgreSQL connection timeout      | Retry with backoff            |
| **DISK_FULL**         | No          | ClickHouse disk at 100%            | Stop DAG, add storage         |
| **PERMISSION_DENIED** | No          | No INSERT rights on table          | Stop DAG, fix permissions     |

### Severity Levels

```
CRITICAL (Stop ETL immediately):
  - NULL_PK
  - SCHEMA_MISMATCH
  - DISK_FULL
  - PERMISSION_DENIED
  → Action: Send email + page on-call

WARNING (Continue ETL, log error):
  - FK_MISS
  - DUPLICATE_PK
  - OUT_OF_RANGE
  - VALIDATION_FAIL
  → Action: Log to error_records, retry later

INFO (Informational only):
  - Data quality statistics
  - Row count differences < 10%
  → Action: Log only, no alert
```

---

## Error Handling in Code

### Complete Example with Try-Catch

```python
def load_fact_with_error_handling(fact_data, processing_batch_id, task_name):
    """Load fact data with comprehensive error tracking."""
    error_records = []
    loaded_records = []
    
    for idx, record in fact_data.iterrows():
        try:
            # Step 1: Resolve foreign keys
            customer_key = lookup_customer_key(record['CustomerID'])
            if not customer_key:
                raise ValueError(f"FK Miss: CustomerID {record['CustomerID']} not found")
            
            product_key = lookup_product_key(record['ProductID'])
            if not product_key:
                raise ValueError(f"FK Miss: ProductID {record['ProductID']} not found")
            
            # Step 2: Validate business rules
            if record['SalesAmount'] < 0:
                raise ValueError(f"Invalid revenue: {record['SalesAmount']}")
            
            if record['Quantity'] <= 0:
                raise ValueError(f"Invalid quantity: {record['Quantity']}")
            
            # Step 3: Insert to ClickHouse
            clickhouse_client.insert_one('FactSales', record)
            loaded_records.append(record)
            
        except KeyError as e:
            # Missing column = schema mismatch (CRITICAL)
            error_record = build_error_record(
                source_table='FactSales',
                record_natural_key=f"{record.get('OrderID')}-{record.get('LineNumber')}",
                error_type='SCHEMA_MISMATCH',
                error_message=f"Missing column: {str(e)}",
                error_severity='CRITICAL',
                failed_data=json.dumps(record.to_dict()),
                processing_batch_id=processing_batch_id,
                task_name=task_name,
                is_recoverable=0  # Cannot recover
            )
            error_records.append(error_record)
            raise  # Stop ETL immediately
            
        except ValueError as e:
            # Business logic error (WARNING)
            error_type = 'FK_MISS' if 'FK Miss' in str(e) else 'VALIDATION_FAIL'
            error_record = build_error_record(
                source_table='FactSales',
                record_natural_key=f"{record['OrderID']}-{record['LineNumber']}",
                error_type=error_type,
                error_message=str(e),
                error_severity='WARNING',
                failed_data=json.dumps(record.to_dict()),
                processing_batch_id=processing_batch_id,
                task_name=task_name,
                is_recoverable=1  # Can retry
            )
            error_records.append(error_record)
            # Continue processing (don't stop)
            
        except Exception as e:
            # Unexpected error (CRITICAL)
            error_record = build_error_record(
                source_table='FactSales',
                record_natural_key=f"{record.get('OrderID')}-{record.get('LineNumber')}",
                error_type='UNEXPECTED_ERROR',
                error_message=str(e),
                error_severity='CRITICAL',
                failed_data=json.dumps(record.to_dict()),
                processing_batch_id=processing_batch_id,
                task_name=task_name,
                is_recoverable=0
            )
            error_records.append(error_record)
            raise  # Stop ETL immediately
    
    # Write error records to ClickHouse
    if error_records:
        clickhouse_client.insert('error_records', error_records)
    
    # Log summary
    logger.info(
        f"Loaded {len(loaded_records)} records, "
        f"{len(error_records)} errors (recoverable: "
        f"{sum(1 for e in error_records if e['IsRecoverable'])}, "
        f"non-recoverable: {sum(1 for e in error_records if not e['IsRecoverable'])})"
    )
    
    return {
        'loaded': len(loaded_records),
        'errors': len(error_records),
        'batch_id': processing_batch_id
    }
```

### Helper Function: Build Error Record

```python
def build_error_record(
    source_table: str,
    record_natural_key: str,
    error_type: str,
    error_message: str,
    failed_data: Any,
    task_name: str,
    batch_id: str,
    error_severity: str = "Warning",
    is_recoverable: int = 1
):
    """Build error record tuple for insertion."""
    error_id = uuid.uuid4().int & ((1 << 64) - 1)
    now = datetime.now()
    
    try:
        failed_json = json.dumps(failed_data, default=str, ensure_ascii=False)
    except Exception:
        failed_json = str(failed_data)
    
    return (
        error_id,                    # ErrorID
        now,                         # ErrorDate
        source_table,                # SourceTable
        record_natural_key,          # RecordNaturalKey
        error_type,                  # ErrorType
        error_severity,              # ErrorSeverity
        error_message,               # ErrorMessage
        failed_json,                 # FailedData
        batch_id,                    # ProcessingBatchID
        task_name,                   # TaskName
        int(is_recoverable),         # IsRecoverable
        0,                           # RetryCount
        None,                        # LastAttemptDate
        0,                           # IsResolved
        None                         # ResolutionComment
    )
```

---

## Retry Strategy

### Exponential Backoff

```
Retry 1: 60 seconds delay
Retry 2: 120 seconds delay (2 × 60)
Max 2 retries total

Timeline:
02:30:00 - Initial failure
02:31:00 - Retry 1 (60s delay)
02:33:00 - Retry 2 (120s delay)
02:41:00 - Give up (mark as non-recoverable)
```

### Implementation

```python
def retry_with_backoff(func, max_retries=3, initial_delay=60):
    """Retry function with exponential backoff."""
    for attempt in range(max_retries):
        try:
            return func()
        except RecoverableError as e:
            if attempt == max_retries - 1:
                # Last attempt failed
                raise
            
            delay = initial_delay * (2 ** attempt)
            logger.warning(f"Attempt {attempt+1} failed, retrying in {delay}s: {e}")
            time.sleep(delay)
    
    raise Exception("Max retries exceeded")
```

---

## Error Reprocessing

### Automatic Reprocessing Task

```python
@task
def reprocess_recoverable_errors(processing_date):
    """Reprocess recoverable errors from previous runs."""
    
    # Query unresolved recoverable errors
    query = """
    SELECT * FROM error_records
    WHERE IsResolved = 0 
      AND IsRecoverable = 1 
      AND RetryCount < 3
      AND ErrorDate >= '{processing_date}' - INTERVAL 1 DAY
    ORDER BY LastAttemptDate ASC
    LIMIT 100
    """.format(processing_date=processing_date)
    
    errors = clickhouse_client.execute(query)
    
    resolved_count = 0
    failed_count = 0
    
    for error in errors:
        try:
            source_table = error['SourceTable']
            record_natural_key = error['RecordNaturalKey']
            failed_data = json.loads(error['FailedData'])
            
            # Attempt to reload with updated data
            if source_table == 'FactSales':
                # Try to resolve FK again (maybe dimension added since first attempt)
                customer_key = lookup_customer_key(failed_data['CustomerID'])
                
                if customer_key:
                    # FK now resolved! Insert fact
                    clickhouse_client.insert_one('FactSales', failed_data)
                    
                    # Mark error as resolved
                    clickhouse_client.command(f"""
                        ALTER TABLE error_records 
                        UPDATE IsResolved=1, ResolutionComment='Auto-resolved on retry'
                        WHERE ErrorID={error['ErrorID']}
                    """)
                    
                    resolved_count += 1
                    logger.info(f"Resolved error {error['ErrorID']}")
                    continue
            
            # Still can't resolve, increment retry count
            clickhouse_client.command(f"""
                ALTER TABLE error_records 
                UPDATE RetryCount = RetryCount + 1,
                       LastAttemptDate = now()
                WHERE ErrorID = {error['ErrorID']}
            """)
            
            failed_count += 1
            
        except Exception as e:
            logger.error(f"Failed to reprocess error {error['ErrorID']}: {str(e)}")
            failed_count += 1
    
    logger.info(f"Reprocessed {len(errors)} errors: {resolved_count} resolved, {failed_count} failed")
    return {"resolved": resolved_count, "failed": failed_count}
```

---

## Alerting Strategy

Basically, no **Alerting Strategy**
is incorporated in the base version.

Although, due to the low complexity of
the code contained within the system, 
incorporating new alerting systems
shouldn't be too complicated.

The only "Alerting System" (not really)
is the ability to plug into the ``error_records``
table within the clickhouse data warehouse.

---

## Monitoring Dashboard

### Key Metrics Query

```sql
-- Daily Error Summary
SELECT 
    ErrorType, 
    COUNT(*) as ErrorCount,
    SUM(IsRecoverable) as Recoverable,
    SUM(1 - IsRecoverable) as NonRecoverable,
    AVG(CAST(IsResolved as Float)) as ResolvedPercent
FROM error_records
WHERE ErrorDate >= today() - INTERVAL 7 DAY
GROUP BY ErrorType
ORDER BY ErrorCount DESC;
```

Sample output:

| ErrorType       | ErrorCount | Recoverable | NonRecoverable | ResolvedPercent |
|-----------------|------------|-------------|----------------|-----------------|
| FK_MISS         | 156        | 156         | 0              | 0.78            |
| VALIDATION_FAIL | 23         | 23          | 0              | 0.95            |
| TIMEOUT         | 2          | 2           | 0              | 1.0             |

### Unresolved Critical Errors

```sql
-- Unresolved Critical Errors (requires immediate attention)
SELECT 
    ErrorID, 
    ErrorDate, 
    SourceTable, 
    ErrorType, 
    ErrorMessage,
    FailedData,
    RetryCount
FROM error_records
WHERE IsResolved = 0 
  AND IsRecoverable = 0  -- Non-recoverable = critical
  AND ErrorDate >= today() - INTERVAL 30 DAY
ORDER BY ErrorDate DESC;
```

### Recovery Success Rate

```sql
-- Recovery Success Rate by Day
SELECT 
    toDate(ErrorDate) as Day,
    COUNT(*) as TotalErrors,
    SUM(IsResolved) as ResolvedErrors,
    ROUND(100.0 * SUM(IsResolved) / COUNT(*), 2) as RecoveryPercent
FROM error_records
WHERE ErrorDate >= today() - INTERVAL 30 DAY
GROUP BY Day
ORDER BY Day DESC;
```
Sample output:

| Day        | TotalErrors | ResolvedErrors | RecoveryPercent |
|------------|-------------|----------------|-----------------|
| 2025-11-11 | 178         | 168            | 94.38           |
| 2025-11-10 | 42          | 42             | 100.0           |
| 2025-11-09 | 95          | 89             | 93.68           |

### Error Trend Chart

```sql
-- Error count by type over last 30 days
SELECT 
    toDate(ErrorDate) as Day,
    ErrorType,
    COUNT(*) as ErrorCount
FROM error_records
WHERE ErrorDate >= today() - INTERVAL 30 DAY
GROUP BY Day, ErrorType
ORDER BY Day DESC, ErrorCount DESC;
```

---

## Summary

**Key Components**:
1. **error_records table** - Centralized error logging
2. **Error classification—**Recoverable vs non-recoverable
3. **Try-catch patterns—**Comprehensive error handling
4. **Retry strategy—**Exponential backoff (60→120→240s)
5. **Reprocessing task—**Automatic error recovery
6. **Alert levels** - Critical/Warning/Info
7. **Monitoring queries** - Track error trends

**Best Practices**:
- Always log errors to error_records
- Classify errors as recoverable/non-recoverable
- Use exponential backoff for retries
- Send critical alerts immediately
- Reprocess recoverable errors daily
- Monitor error trends weekly

**Next Steps**:
- See [Transformation Logic](transformation_logic.md) for FK resolution
- See [Airflow DAG Spec](airflow_dag_spec.md) for retry configuration
- See [Deployment Runbook](deployment_runbook.md) for incident response