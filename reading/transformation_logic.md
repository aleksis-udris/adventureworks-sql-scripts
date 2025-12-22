# Transformation Logic Specification

## Table of Contents
1. [SCD Type 2 Merge Logic](#scd-type-2-merge-logic)
2. [SCD Type 1 Upsert](#scd-type-1-upsert)
3. [Fact Table Loading](#fact-table-loading)
4. [Aggregate Calculations](#aggregate-calculations)
5. [Data Quality Checks](#data-quality-checks)

---

## SCD Type 2 Merge Logic

### Overview

SCD Type 2 preserves full history of dimension changes by creating new rows when attributes change.

### Example: Customer Email Change

**Before Change**:
```
CustomerKey | CustomerID | Email | ValidFromDate | ValidToDate | IsCurrent
1001 | 500 | old@example.com | 2020-01-15 | NULL | 1
```

**After Change**:
```
CustomerKey | CustomerID | Email | ValidFromDate | ValidToDate | IsCurrent
1001 | 500 | old@example.com | 2020-01-15 | 2025-11-10 | 0
1002 | 500 | new@example.com | 2025-11-11 | NULL | 1
```

### Implementation Steps

**Step 1: Detect Changes**

```sql
SELECT src.customer_id, src.email, dim.email as old_email
FROM stg_customers src
JOIN DimCustomer dim ON src.customer_id = dim.customer_id AND dim.IsCurrent = 1
WHERE src.email != dim.email;
```

Result: Customer 500 has email change detected

**Step 2: End-date Old Record**

```sql
UPDATE DimCustomer
SET ValidToDate = CURRENT_DATE - 1,
    IsCurrent = 0
WHERE customer_id = 500 AND IsCurrent = 1;
```

Old row now: ValidToDate=2025-11-10, IsCurrent=0

**Step 3: Insert New Record**

```sql
INSERT INTO DimCustomer (
    CustomerKey, CustomerID, Email, ValidFromDate, ValidToDate, IsCurrent, Version
)
SELECT 
    NEXTVAL('customer_key_seq'),
    customer_id,
    email,
    CURRENT_DATE,
    NULL,
    1,
    (SELECT MAX(Version) + 1 FROM DimCustomer WHERE CustomerID = 500)
FROM stg_customers
WHERE customer_id = 500;
```

New row: CustomerKey=1002, Email=new@example.com, ValidFromDate=2025-11-11, IsCurrent=1

### Change Detection Rules

Monitor these columns for changes:
- Email changed: New row
- City changed: New row
- Country changed: New row
- CustomerSegment changed: New row
- AccountStatus changed: New row

If ANY tracked column changes, create new version.

### Python Implementation

```python
def apply_scd_type_2(pg_map, ch_map, columns, natural_key, tracked_columns):
    """
    Apply SCD Type 2 logic by comparing PostgreSQL source with ClickHouse warehouse.
    
    Args:
        pg_map: Dict of {natural_key -> row} from source
        ch_map: Dict of {natural_key -> row} from warehouse
        columns: List of column names
        natural_key: Name of natural key column
        tracked_columns: Columns to monitor for changes
    
    Returns:
        to_insert: New rows to insert
        to_expire: Keys of rows to expire (set IsCurrent=0)
    """
    idx = {c: i for i, c in enumerate(columns)}
    nk_idx = idx[natural_key]
    
    to_insert = []
    to_expire = []
    
    for key in pg_map.keys():
        if key not in ch_map:
            # New record: insert with IsCurrent=1
            row = list(pg_map[key])
            row[idx['IsCurrent']] = 1
            row[idx['ValidFromDate']] = date.today()
            row[idx['ValidToDate']] = None
            to_insert.append(row)
        else:
            # Existing record: check for changes
            pg_row = pg_map[key]
            ch_row = ch_map[key]
            
            changed = False
            for col in tracked_columns:
                if pg_row[idx[col]] != ch_row[idx[col]]:
                    changed = True
                    break
            
            if changed:
                # Create new version
                new_row = list(pg_map[key])
                new_row[idx['CustomerKey']] = generate_surrogate_key()
                new_row[idx['IsCurrent']] = 1
                new_row[idx['ValidFromDate']] = date.today()
                new_row[idx['ValidToDate']] = None
                new_row[idx['Version']] = ch_row[idx['Version']] + 1
                to_insert.append(new_row)
                
                # Mark old version for expiration
                to_expire.append(key)
    
    return to_insert, to_expire
```

---

## SCD Type 1 Upsert

### Overview

SCD Type 1 overwrites old values without keeping history.

### Example: DimDate

```sql
INSERT INTO DimDate (DateKey, FullDate, Year, Month, MonthName, ...)
SELECT 
    CAST(FORMAT(FullDate, 'yyyyMMdd') as Integer) as DateKey,
    FullDate,
    EXTRACT(YEAR FROM FullDate),
    EXTRACT(MONTH FROM FullDate),
    TO_CHAR(FullDate, 'Month'),
    ...
FROM calendar_source
WHERE FullDate = CURRENT_DATE
ON CONFLICT (DateKey) DO UPDATE SET
    Year = EXCLUDED.Year,
    Month = EXCLUDED.Month,
    MonthName = EXCLUDED.MonthName;
```

If date already exists, update attributes. No new row created.

---

## Fact Table Loading

### Overview

Facts are loaded by resolving foreign keys from dimensions and validating measures.

### FactSales Example

**Step 1: Extract from Source**

```sql
SELECT 
    soh.OrderDate::DATE AS SalesDateKey,
    c.CustomerID AS CustomerID,
    p.ProductID AS ProductID,
    sod.OrderQty AS QuantitySold,
    sod.UnitPrice * sod.OrderQty AS SalesRevenue
FROM Sales.SalesOrderDetail sod
JOIN Sales.SalesOrderHeader soh ON sod.SalesOrderID = soh.SalesOrderID
JOIN Sales.Customer c ON soh.CustomerID = c.CustomerID
JOIN Production.Product p ON sod.ProductID = p.ProductID;
```

**Step 2: Resolve Foreign Keys**

```python
def resolve_foreign_keys(source_rows, ch_client):
    """
    Resolve natural keys to surrogate keys.
    
    Example:
        CustomerID=500 -> CustomerKey=1002 (current version)
        ProductID=123 -> ProductKey=456 (current version)
    """
    valid_rows = []
    error_rows = []
    
    for row in source_rows:
        try:
            # Lookup CustomerKey
            customer_key = ch_client.query(
                f"SELECT CustomerKey FROM DimCustomer "
                f"WHERE CustomerID = {row['CustomerID']} AND IsCurrent = 1"
            ).first_row
            
            if not customer_key:
                raise ValueError(f"Customer {row['CustomerID']} not found")
            
            # Lookup ProductKey
            product_key = ch_client.query(
                f"SELECT ProductKey FROM DimProduct "
                f"WHERE ProductID = {row['ProductID']} AND IsCurrent = 1"
            ).first_row
            
            if not product_key:
                raise ValueError(f"Product {row['ProductID']} not found")
            
            # Build fact row
            fact_row = {
                'SalesDateKey': row['SalesDateKey'],
                'CustomerKey': customer_key[0],
                'ProductKey': product_key[0],
                'QuantitySold': row['QuantitySold'],
                'SalesRevenue': row['SalesRevenue']
            }
            
            valid_rows.append(fact_row)
            
        except ValueError as e:
            error_rows.append({
                'row': row,
                'error': str(e),
                'error_type': 'FK_MISS'
            })
    
    return valid_rows, error_rows
```

**Step 3: Validate Measures**

```python
def validate_measures(row):
    """Validate business rules on measures."""
    errors = []
    
    if row['SalesRevenue'] < 0:
        errors.append('Negative revenue not allowed')
    
    if row['QuantitySold'] <= 0:
        errors.append('Quantity must be positive')
    
    if row['SalesRevenue'] < row['QuantitySold'] * 0.01:
        errors.append('Unit price suspiciously low')
    
    return errors
```

**Step 4: Insert to ClickHouse**

```python
def load_fact_table(valid_rows, ch_client):
    """Insert facts in batches."""
    batch_size = 10000
    
    for i in range(0, len(valid_rows), batch_size):
        batch = valid_rows[i:i + batch_size]
        ch_client.insert('FactSales', batch)
```

### Complete Flow

```python
# Extract
source_rows = extract_from_postgres()

# Transform
valid_rows, error_rows = resolve_foreign_keys(source_rows, ch_client)

# Validate
for row in valid_rows:
    validation_errors = validate_measures(row)
    if validation_errors:
        error_rows.append({'row': row, 'errors': validation_errors})
        valid_rows.remove(row)

# Load
load_fact_table(valid_rows, ch_client)

# Log errors
if error_rows:
    insert_error_records(ch_client, error_rows)
```

---

## Aggregate Calculations

### agg_daily_sales Example

Pre-compute daily sales by store and product category.

**Source Query**:

```sql
INSERT INTO agg_daily_sales (
    SalesDateKey, StoreKey, ProductCategoryKey,
    TotalRevenue, TotalQuantity, TotalDiscount, TransactionCount
)
SELECT 
    fs.SalesDateKey,
    fs.StoreKey,
    dpc.ProductCategoryKey,
    SUM(fs.SalesRevenue) as TotalRevenue,
    SUM(fs.QuantitySold) as TotalQuantity,
    SUM(fs.DiscountAmount) as TotalDiscount,
    COUNT(*) as TransactionCount
FROM FactSales fs
JOIN DimProduct dp ON fs.ProductKey = dp.ProductKey AND dp.IsCurrent = 1
JOIN DimProductCategory dpc ON dp.Category = dpc.CategoryName
WHERE fs.SalesDateKey = CAST(FORMAT(CURRENT_DATE - 1, 'yyyyMMdd') as Integer)
GROUP BY fs.SalesDateKey, fs.StoreKey, dpc.ProductCategoryKey;
```

**Example Output**:

```
SalesDateKey | StoreKey | ProductCategoryKey | TotalRevenue | TotalQuantity
20250111 | 5 | 3 | 152450.75 | 1245
20250111 | 5 | 7 | 98230.50 | 523
```

On Jan 11, Store 5 sold $152K of category 3 products and $98K of category 7.

### Materialized View Pattern

```sql
CREATE MATERIALIZED VIEW mv_agg_daily_sales
TO agg_daily_sales
AS
SELECT 
    fs.SalesDateKey,
    fs.StoreKey,
    dpc.ProductCategoryKey,
    SUM(fs.SalesRevenue) as TotalRevenue,
    SUM(fs.QuantitySold) as TotalQuantity
FROM FactSales fs
JOIN DimProduct dp ON fs.ProductKey = dp.ProductKey
JOIN DimProductCategory dpc ON dp.Category = dpc.CategoryName
GROUP BY fs.SalesDateKey, fs.StoreKey, dpc.ProductCategoryKey;
```

Automatically updates when FactSales changes.

---

## Data Quality Checks

### NULL Handling

```python
def handle_nulls(row, rules):
    """
    Apply null handling rules.
    
    Rules:
        CustomerName NULL -> 'UNKNOWN'
        Email NULL -> '' (empty string)
        Age NULL -> -1 (sentinel value)
        Amount NULL -> 0
        Flag NULL -> 0 (false)
    """
    for column, default_value in rules.items():
        if row[column] is None:
            row[column] = default_value
    
    return row
```

### Deduplication

```sql
-- Remove duplicates based on natural key
SELECT DISTINCT * FROM stg_customers;

-- Or keep only first occurrence
SELECT * FROM stg_customers 
GROUP BY customer_id 
HAVING COUNT(*) = 1;
```

### Range Validation

```python
def validate_ranges(row):
    """Validate values are within acceptable ranges."""
    errors = []
    
    if row['Age'] < 0 or row['Age'] > 120:
        errors.append('Age out of range')
    
    if row['Quantity'] < 0:
        errors.append('Negative quantity')
    
    if row['Price'] < 0:
        errors.append('Negative price')
    
    if row['Price'] < row['Cost']:
        errors.append('Price below cost')
    
    return errors
```

### Type Conversion

```python
def convert_types(row, schema):
    """Convert string values to appropriate types."""
    for column, target_type in schema.items():
        try:
            if target_type == 'int':
                row[column] = int(row[column])
            elif target_type == 'float':
                row[column] = float(row[column])
            elif target_type == 'date':
                row[column] = datetime.strptime(row[column], '%Y-%m-%d').date()
        except (ValueError, TypeError) as e:
            raise ValueError(f"Cannot convert {column} to {target_type}: {e}")
    
    return row
```

---

## Summary

**SCD Type 2 Process**:
1. Detect changes in tracked columns
2. Expire old version (IsCurrent=0, ValidToDate=yesterday)
3. Insert new version (IsCurrent=1, ValidFromDate=today)

**Fact Loading Process**:
1. Extract from source
2. Resolve foreign keys (natural -> surrogate)
3. Validate measures (business rules)
4. Insert to ClickHouse in batches
5. Log errors to error_records

**Aggregation Process**:
1. Join facts with dimensions
2. Group by aggregation keys
3. Calculate measures (SUM, COUNT, AVG)
4. Insert to aggregate table

**Data Quality**:
- Handle nulls with defaults
- Deduplicate by natural key
- Validate ranges and types
- Convert data types safely