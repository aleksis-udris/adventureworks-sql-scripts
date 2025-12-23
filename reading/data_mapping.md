# Data Mapping & Transformation Rules

## Table of Contents
1. [Overview](#overview)
2. [Source System (PostgreSQL)](#source-system-postgresql)
3. [Target System (ClickHouse)](#target-system-clickhouse)
4. [Dimension Mappings](#dimension-mappings)
5. [Fact Table Mappings](#fact-table-mappings)
6. [Transformation Rules](#transformation-rules)
7. [Data Flow Diagrams](#data-flow-diagrams)

---

## Overview

This document describes the mapping between source PostgreSQL AdventureWorks database and target ClickHouse data warehouse, including all transformation rules and data lineage.

**Source System**: PostgreSQL AdventureWorks (OLTP database)  
**Target System**: ClickHouse ADVENTUREWORKS_DWS and ADVENTUREWORKS_DS (Data Warehouse)

---

## Source System (PostgreSQL)

### Database: AdventureWorks

**Schema Structure**:
- `Sales` - Sales orders, customers, stores
- `Production` - Products, inventory, manufacturing
- `Purchasing` - Purchase orders, vendors
- `Person` - Employee and customer personal information
- `HumanResources` - Employee data

**Key Source Tables**:
- `Sales.SalesOrderHeader` - Sales order headers
- `Sales.SalesOrderDetail` - Sales order line items
- `Sales.Customer` - Customer master data
- `Production.Product` - Product master data
- `Production.ProductInventory` - Inventory snapshots
- `Purchasing.PurchaseOrderHeader` - Purchase order headers
- `Purchasing.PurchaseOrderDetail` - Purchase order line items

---

## Target System (ClickHouse)

### Database: ADVENTUREWORKS_DWS (Data Warehouse Store)

**Fact Tables** (9):
- FactSales
- FactPurchases
- FactInventory
- FactProduction
- FactEmployeeSales
- FactCustomerFeedback
- FactPromotionResponse
- FactFinance
- FactReturns

**Dimension Tables** (16):
- DimCustomer, DimProduct, DimStore, DimEmployee, DimDate, DimVendor, DimWarehouse
- DimPromotion, DimSalesTerritory, DimProductCategory, DimFeedbackCategory
- DimReturnReason, DimCustomerSegment, DimAgingTier, DimFinanceCategory, DimRegion

### Database: ADVENTUREWORKS_DS (Data Store)

**Aggregate Tables** (6):
- agg_daily_sales
- agg_weekly_sales
- agg_monthly_sales
- agg_daily_inventory
- agg_monthly_product_performance
- agg_regional_sales

---

## Dimension Mappings

### DimCustomer

**Source**: `Sales.Customer`, `Person.Person`, `Person.EmailAddress`, `Person.Address`

**Key Transformations**:
- CustomerKey: MD5 hash of CustomerID + ModifiedDate (surrogate key)
- CustomerName: Concatenates FirstName + MiddleName + LastName for individuals, or Store Name for businesses
- Email: From `Person.EmailAddress.EmailAddress`
- Address: From `Person.Address` joined through `Person.BusinessEntityAddress`
- CustomerSegment: Derived from `Sales.vPersonDemographics.YearlyIncome`
- AccountStatus: Derived from order history (Active if orders in last year, Inactive otherwise)

**SCD Type**: Type 2 (full history)

---

### DimProduct

**Source**: `Production.Product`, `Production.ProductCategory`, `Production.ProductSubcategory`

**Key Transformations**:
- ProductKey: MD5 hash of ProductID + ModifiedDate (surrogate key)
- SKU: Maps from `Product.ProductNumber`
- Category: From `Production.ProductCategory.Name`
- SubCategory: From `Production.ProductSubcategory.Name`
- Cost: Maps from `Product.StandardCost`
- ListPrice: Maps from `Product.ListPrice`
- ProductStatus: Derived from `DiscontinuedDate` and `SellStartDate`

**SCD Type**: Type 2 (full history)

---

## Fact Table Mappings

### FactSales

**Source**: `Sales.SalesOrderDetail` + `Sales.SalesOrderHeader`

**Grain**: One row per order line item (SalesOrderID + SalesOrderDetailID)

**Key Mappings**:
- SalesDateKey: `SalesOrderHeader.OrderDate::DATE`
- CustomerKey: Resolved from `SalesOrderHeader.CustomerID` → DimCustomer.CustomerKey (IsCurrent=1)
- ProductKey: Resolved from `SalesOrderDetail.ProductID` → DimProduct.ProductKey (IsCurrent=1)
- StoreKey: Resolved from `SalesOrderHeader.Customer.StoreID` → DimStore.StoreKey (IsCurrent=1)
- EmployeeKey: Resolved from `SalesOrderHeader.SalesPersonID` → DimEmployee.EmployeeKey (IsCurrent=1)
- SalesOrderID: Direct mapping
- SalesOrderDetailID: Direct mapping
- QuantitySold: `SalesOrderDetail.OrderQty`
- SalesRevenue: `SalesOrderDetail.UnitPrice * OrderQty`
- DiscountAmount: `SalesOrderDetail.UnitPrice * OrderQty * UnitPriceDiscount`

---

### FactPurchases

**Source**: `Purchasing.PurchaseOrderDetail` + `Purchasing.PurchaseOrderHeader`

**Grain**: One row per purchase order line item (PurchaseOrderID + PurchaseOrderDetailID)

**Key Mappings**:
- PurchaseDateKey: `PurchaseOrderHeader.OrderDate::DATE`
- VendorKey: Resolved from `PurchaseOrderHeader.VendorID` → DimVendor.VendorKey (IsCurrent=1)
- ProductKey: Resolved from `PurchaseOrderDetail.ProductID` → DimProduct.ProductKey (IsCurrent=1)
- QuantityBought: `PurchaseOrderDetail.OrderQty`
- PurchaseAmount: `PurchaseOrderDetail.UnitPrice * OrderQty`

---

### FactInventory

**Source**: `Production.ProductInventory` + derived calculations

**Grain**: One row per date + product + store + warehouse combination

**Key Mappings**:
- InventoryDateKey: ETL processing date (daily snapshot)
- ProductKey: Resolved from `ProductInventory.ProductID` → DimProduct.ProductKey (IsCurrent=1)
- StoreKey: Resolved from location → DimStore.StoreKey (IsCurrent=1)
- WarehouseKey: Resolved from location → DimWarehouse.WarehouseKey (IsCurrent=1)
- QuantityOnHand: `ProductInventory.Quantity`
- StockAging: Calculated from last movement date
- ReorderLevel: From `ProductInventory.Location.ReorderLevel`
- SafetyStockLevels: From `ProductInventory.Location.SafetyStockLevel`

---

## Transformation Rules

### Surrogate Key Generation

**Method**: MD5 hash of (NaturalKey + ModifiedDate) converted to 64-bit integer

```python
CustomerKey = int(md5(f"{CustomerID}{ModifiedDate}").hexdigest()[:16], 16)
```

### SCD Type 2 Logic

**For Dimensions** (Customer, Product, Store, Employee, Vendor, Warehouse):

1. Extract current records from PostgreSQL
2. Load existing records from ClickHouse (WHERE IsCurrent=1)
3. Compare tracked columns (Email, City, Country, etc.)
4. If changed:
   - Update old version: Set IsCurrent=0, ValidToDate=yesterday
   - Insert new version: IsCurrent=1, ValidFromDate=today, increment Version

### Foreign Key Resolution

**For Fact Tables**:

1. Collect all natural keys (CustomerID, ProductID, etc.) from source
2. Bulk lookup surrogate keys from dimension tables:
   ```sql
   SELECT CustomerID, CustomerKey 
   FROM DimCustomer 
   WHERE CustomerID IN (...list...) AND IsCurrent = 1
   ```
3. Map natural keys to surrogate keys
4. If FK missing: Log to error_records, skip row (recoverable error)

### Date Dimension

**Source**: Generated in ClickHouse (not from PostgreSQL)

**Generation**: Materialized view `DimDate_MV` creates calendar dates from 2011-01-01 to 10 years in the future

---

## Data Flow Diagrams

### Dimension Loading Flow

```
PostgreSQL Source
    ↓
Extract (SELECT query)
    ↓
Transform (Generate surrogate keys, apply defaults)
    ↓
Compare with ClickHouse (SCD Type 2 detection)
    ↓
Insert/Update ClickHouse Dimension Table
    ↓
OPTIMIZE TABLE (merge parts)
```

### Fact Loading Flow

```
PostgreSQL Source
    ↓
Extract (SELECT query with joins)
    ↓
Transform (Clean data, convert types)
    ↓
Resolve Foreign Keys (bulk lookup from dimensions)
    ↓
Validate (Business rules, NULL checks)
    ↓
Insert to ClickHouse Fact Table (batch inserts)
    ↓
Log Errors (to error_records if any)
```

### Aggregate Loading Flow

```
Fact Tables (ADVENTUREWORKS_DWS)
    ↓
Materialized Views (auto-incremental)
    ↓
Aggregate Tables (ADVENTUREWORKS_DS)
    ↓
Reporting & Dashboards
```

---

## Summary

**Key Mapping Principles**:
- Surrogate keys: MD5 hash of natural key + modified date
- SCD Type 2: Full history preservation for dimensions
- Foreign keys: Resolved via bulk lookup with IsCurrent=1 filter
- Daily partitions: Fine-grained pruning for date-range queries
- Error handling: All FK misses logged to error_records for reprocessing

**Next Steps**:
- See [Transformation Logic](transformation_logic.md) for detailed SCD Type 2 implementation
- See [ETL Architecture](etl_architecture.md) for pipeline overview
- See [Schema Specification](schema_specification.md) for table structures