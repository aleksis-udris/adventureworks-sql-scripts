# Schema Design Specification

## Table of Contents
1. [Overview](#overview)
2. [Star Schema Design](#star-schema-design)
3. [Fact Tables](#fact-tables)
4. [Dimension Tables](#dimension-tables)
5. [Grain Definitions](#grain-definitions)
6. [Key Relationships](#key-relationships)

---

## Overview

### Schema Type

**Star Schema** with conforming dimensions.

```
       DimCustomer ────┐
       DimProduct ─────┤
       DimStore ───────┼──── FactSales
       DimEmployee ────┤
       DimDate ────────┘
```

**Why Star Schema?**
- Simple, intuitive structure
- Fast queries (few joins to small dimensions)
- Flexible for different analysis needs
- Easy to understand for analysts

**vs Snowflake Schema**:
```
Star:      FactSales → DimProduct (Category included)
Snowflake: FactSales → DimProduct → DimCategory (normalized)

Star: 1 join, faster
Snowflake: 2 joins, slower but less redundancy
```

---

## Star Schema Design

### Complete Schema Diagram

```
┌────────────────────────────────────────────────────────────────┐
│                        FACT TABLES                             │
├────────────────────────────────────────────────────────────────┤
│ FactSales            FactPurchases       FactInventory         │
│ FactProduction       FactEmployeeSales   FactCustomerFeedback  │
│ FactPromotionResponse FactFinance        FactReturns           │
└────────────────────────────────────────────────────────────────┘
                                ↓
                    Connected via Foreign Keys
                                ↓
┌────────────────────────────────────────────────────────────────┐
│                     DIMENSION TABLES                           │
├────────────────────────────────────────────────────────────────┤
│ DimCustomer (SCD2)    DimProduct (SCD2)    DimStore (SCD2)    │
│ DimEmployee (SCD2)    DimVendor (SCD2)     DimWarehouse (SCD2)│
│ DimDate (SCD1)        DimPromotion         DimSalesTerritory  │
│ DimProductCategory    DimFeedbackCategory  DimReturnReason    │
│ DimCustomerSegment    DimAgingTier         DimFinanceCategory │
│ DimRegion                                                      │
└────────────────────────────────────────────────────────────────┘
```

**Legend**:
- **SCD Type 1**: Overwrite (no history)
- **SCD Type 2**: Full history with versioning

---

## Fact Tables

### 1. FactSales

**Purpose**: Track sales transactions at line item level.

**Grain**: One row per order line item (SalesOrderID + SalesOrderDetailID).

```sql
CREATE TABLE FactSales (
    -- Foreign Keys
    SalesDateKey Date,
    CustomerKey UInt64,
    ProductKey UInt64,
    StoreKey UInt64,
    EmployeeKey UInt64,
    
    -- Grain
    SalesOrderID UInt32,
    SalesOrderDetailID UInt32,
    
    -- Measures
    QuantitySold UInt32,
    SalesRevenue Decimal(18, 2),
    DiscountAmount Decimal(18, 2),
    NumberOfTransactions UInt8,
    
    -- Degenerate Dimensions
    UnitPrice Decimal(18, 2),
    UnitPriceDiscount Decimal(18, 4),
    LineTotal Decimal(18, 2),
    InsertedAt DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY SalesDateKey
ORDER BY (SalesDateKey, ProductKey, CustomerKey, StoreKey, SalesOrderID, SalesOrderDetailID);
```

**Example Row**:
```
SalesDateKey: 2025-11-11
CustomerKey: 1002
ProductKey: 456
QuantitySold: 2
SalesRevenue: 450.50
```

**Typical Queries**:
```sql
-- Daily sales by product
SELECT SalesDateKey, ProductKey, SUM(SalesRevenue)
FROM FactSales
WHERE SalesDateKey >= '2025-11-01'
GROUP BY SalesDateKey, ProductKey;
```

### 2. FactPurchases

**Purpose**: Track purchases from vendors.

**Grain**: One row per purchase order line (PurchaseOrderID + PurchaseOrderDetailID).

```sql
CREATE TABLE FactPurchases (
    -- Foreign Keys
    PurchaseDateKey Date,
    VendorKey UInt64,
    ProductKey UInt64,
    
    -- Grain
    PurchaseOrderID UInt32,
    PurchaseOrderDetailID UInt32,
    
    -- Measures
    QuantityBought UInt32,
    PurchaseAmount Decimal(18, 2),
    DiscountAmount Decimal(18, 2),
    
    -- Degenerate Dimensions
    UnitPrice Decimal(18, 2),
    UnitPriceDiscount Decimal(18, 4),
    LineTotal Decimal(18, 2)
) ENGINE = MergeTree()
PARTITION BY PurchaseDateKey
ORDER BY (PurchaseDateKey, ProductKey, VendorKey, PurchaseOrderID, PurchaseOrderDetailID);
```

### 3. FactInventory

**Purpose**: Daily inventory snapshots by product, store, and warehouse.

**Grain**: One row per date + product + store + warehouse combination.

```sql
CREATE TABLE FactInventory (
    -- Foreign Keys
    InventoryDateKey Date,
    ProductKey UInt64,
    StoreKey UInt64,
    WarehouseKey UInt64,
    
    -- Measures
    QuantityOnHand UInt32,
    StockAging UInt32,
    ReorderLevel UInt32,
    SafetyStockLevels UInt32,
    
    -- Metadata
    SnapshotCreatedDateTime DateTime,
    ETLBatchID String
) ENGINE = MergeTree()
PARTITION BY InventoryDateKey
ORDER BY (InventoryDateKey, ProductKey, WarehouseKey, StoreKey);
```

**Example**:
```
InventoryDateKey: 2025-11-11
ProductKey: 456
WarehouseKey: 3
QuantityOnHand: 1500
StockAging: 45 (days)
```

### 4. FactProduction

**Purpose**: Track production runs and efficiency.

**Grain**: One row per production run.

```sql
CREATE TABLE FactProduction (
    -- Surrogate Key
    ProductionRunID UInt64,
    
    -- Foreign Keys
    ProductionDateKey Date,
    ProductKey UInt64,
    SupervisorKey UInt64,
    
    -- Measures
    UnitsProduced UInt32,
    ProductionTimeHours Decimal(10, 2),
    ScrapRatePercent Decimal(5, 2),
    DefectCount UInt32,
    
    -- Metadata
    ETLBatchID String,
    LoadTimestamp DateTime
) ENGINE = MergeTree()
PARTITION BY ProductionDateKey
ORDER BY (ProductionDateKey, ProductKey, ProductionRunID);
```

### 5. FactEmployeeSales

**Purpose**: Track employee sales performance.

**Grain**: One row per date + employee + store + sales territory combination.

```sql
CREATE TABLE FactEmployeeSales (
    -- Foreign Keys
    SalesDateKey Date,
    EmployeeKey UInt64,
    StoreKey UInt64,
    SalesTerritoryKey UInt64,
    
    -- Measures
    SalesAmount Decimal(18, 2),
    SalesTarget Decimal(18, 2),
    TargetAttainment Decimal(10, 4),
    CustomerContactsCount UInt32,
    
    -- Metadata
    ETLBatchID String,
    LoadTimestamp DateTime
) ENGINE = MergeTree()
PARTITION BY SalesDateKey
ORDER BY (SalesDateKey, EmployeeKey, StoreKey, SalesTerritoryKey);
```

### 6. FactCustomerFeedback

**Purpose**: Track customer feedback and satisfaction.

**Grain**: One row per feedback submission.

```sql
CREATE TABLE FactCustomerFeedback (
    -- Foreign Keys
    FeedbackDateKey Date,
    CustomerKey UInt64,
    EmployeeKey UInt64,
    FeedbackCategoryKey UInt64,
    
    -- Measures
    FeedbackScore UInt8,
    ComplaintCount UInt8,
    ResolutionTimeHours Decimal(10, 2),
    CSATScore Decimal(5, 2),
    Comments String,
    Channel LowCardinality(String),
    
    -- Metadata
    ETLBatchID String,
    LoadTimestamp DateTime
) ENGINE = MergeTree()
PARTITION BY FeedbackDateKey
ORDER BY (FeedbackDateKey, CustomerKey, EmployeeKey, FeedbackCategoryKey);
```

### 7. FactPromotionResponse

**Purpose**: Track promotion campaign effectiveness.

**Grain**: One row per date + product + store + promotion.

```sql
CREATE TABLE FactPromotionResponse (
    -- Foreign Keys
    PromotionDateKey Date,
    ProductKey UInt64,
    StoreKey UInt64,
    PromotionKey UInt64,
    
    -- Measures
    SalesDuringCampaign Decimal(18, 2),
    DiscountUsageCount UInt32,
    CustomerUptakeRate Decimal(10, 4),
    PromotionROI Decimal(10, 4),
    
    -- Metadata
    ETLBatchID String,
    LoadTimestamp DateTime
) ENGINE = MergeTree()
PARTITION BY PromotionDateKey
ORDER BY (PromotionDateKey, ProductKey, PromotionKey, StoreKey);
```

### 8. FactFinance

**Purpose**: Track financial transactions and invoices.

**Grain**: One row per invoice.

```sql
CREATE TABLE FactFinance (
    -- Foreign Keys
    InvoiceDateKey Date,
    CustomerKey UInt64,
    StoreKey UInt64,
    FinanceCategoryKey UInt64,
    
    -- Measures
    InvoiceAmount Decimal(18, 2),
    PaymentDelayDays Int32,
    CreditUsagePct Decimal(10, 4),
    InterestCharges Decimal(18, 2),
    
    -- Degenerate Dimensions
    InvoiceNumber String,
    PaymentStatus LowCardinality(String),
    CurrencyCode LowCardinality(String),
    
    -- Metadata
    ETLBatchID String,
    LoadTimestamp DateTime
) ENGINE = MergeTree()
PARTITION BY InvoiceDateKey
ORDER BY (InvoiceDateKey, CustomerKey, StoreKey, FinanceCategoryKey);
```

### 9. FactReturns

**Purpose**: Track product returns.

**Grain**: One row per return transaction.

```sql
CREATE TABLE FactReturns (
    -- Foreign Keys
    ReturnDateKey Date,
    ProductKey UInt64,
    CustomerKey UInt64,
    StoreKey UInt64,
    ReturnReasonKey UInt64,
    
    -- Measures
    ReturnedQuantity UInt32,
    RefundAmount Decimal(18, 2),
    RestockingFee Decimal(18, 2),
    
    -- Degenerate Dimensions
    ReturnID String,
    OriginalSalesID String,
    ReturnMethod LowCardinality(String),
    ConditionOnReturn LowCardinality(String),
    
    -- Metadata
    ETLBatchID String,
    LoadTimestamp DateTime
) ENGINE = MergeTree()
PARTITION BY ReturnDateKey
ORDER BY (ReturnDateKey, ProductKey, CustomerKey, StoreKey, ReturnReasonKey);
```

---

## Dimension Tables

### SCD Type 2 Dimensions (Full History)

#### 1. DimCustomer

**Purpose**: Customer master data with full history tracking.

**SCD Type**: Type 2 (versions preserved).

```sql
CREATE TABLE DimCustomer (
    -- Keys
    CustomerKey UInt64,          -- Surrogate key (unique per version)
    CustomerID UInt32,           -- Natural key (same across versions)
    
    -- Attributes
    CustomerName String,
    Email String,
    Phone String,
    City LowCardinality(String),
    StateProvince LowCardinality(String),
    Country LowCardinality(String),
    PostalCode String,
    CustomerSegment LowCardinality(String),
    CustomerType LowCardinality(String),
    AccountStatus LowCardinality(String),
    CreditLimit Decimal(18, 2),
    AnnualIncome Decimal(18, 2),
    YearsSinceFirstPurchase Int32,
    
    -- Version Control
    ValidFromDate Date,
    ValidToDate Nullable(Date),
    IsCurrent UInt8,
    SourceUpdateDate Date,
    EffectiveStartDate Date,
    EffectiveEndDate Nullable(Date),
    Version UInt64
) ENGINE = ReplacingMergeTree(Version)
PARTITION BY toYYYYMM(ValidFromDate)
ORDER BY (CustomerID, ValidFromDate);
```

**Example History**:
```
CustomerKey | CustomerID | Email            | ValidFromDate | ValidToDate | IsCurrent
1001        | 500        | old@example.com  | 2020-01-15    | 2025-11-10  | 0
1002        | 500        | new@example.com  | 2025-11-11    | NULL        | 1
```

#### 2. DimProduct

**Purpose**: Product master data with history.

**SCD Type**: Type 2.

```sql
CREATE TABLE DimProduct (
    -- Keys
    ProductKey UInt64,
    ProductID UInt32,
    
    -- Attributes
    ProductName String,
    SKU String,
    Category LowCardinality(String),
    SubCategory LowCardinality(String),
    Brand LowCardinality(String),
    ListPrice Decimal(18, 2),
    Cost Decimal(18, 2),
    ProductStatus LowCardinality(String),
    Color LowCardinality(String),
    Size LowCardinality(String),
    Weight Decimal(10, 3),
    
    -- Version Control
    ValidFromDate Date,
    ValidToDate Nullable(Date),
    IsCurrent UInt8,
    SourceUpdateDate Date,
    EffectiveStartDate Date,
    EffectiveEndDate Nullable(Date),
    Version UInt64
) ENGINE = ReplacingMergeTree(Version)
PARTITION BY toYYYYMM(ValidFromDate)
ORDER BY (ProductID, ValidFromDate);
```

#### 3. DimStore

**Purpose**: Store location data with history.

**SCD Type**: Type 2.

```sql
CREATE TABLE DimStore (
    -- Keys
    StoreKey UInt64,
    StoreID UInt32,
    
    -- Attributes
    StoreName String,
    StoreNumber UInt32,
    Address String,
    City LowCardinality(String),
    StateProvince LowCardinality(String),
    Country LowCardinality(String),
    PostalCode String,
    Region LowCardinality(String),
    Territory LowCardinality(String),
    StoreType LowCardinality(String),
    StoreStatus LowCardinality(String),
    ManagerName String,
    OpeningDate Date,
    SquareFootage UInt32,
    
    -- Version Control
    ValidFromDate Date,
    ValidToDate Nullable(Date),
    IsCurrent UInt8,
    SourceUpdateDate Date,
    Version UInt64
) ENGINE = ReplacingMergeTree(Version)
PARTITION BY toYYYYMM(ValidFromDate)
ORDER BY (StoreID, ValidFromDate);
```

#### 4. DimEmployee

**Purpose**: Employee data with job history.

**SCD Type**: Type 2.

```sql
CREATE TABLE DimEmployee (
    -- Keys
    EmployeeKey UInt64,
    EmployeeID Int32,
    
    -- Attributes
    EmployeeName String,
    JobTitle LowCardinality(String),
    Department LowCardinality(String),
    ReportingManagerKey UInt64,
    HireDate Date,
    EmployeeStatus LowCardinality(String),
    Region LowCardinality(String),
    Territory LowCardinality(String),
    SalesQuota Decimal(18,2),
    
    -- Version Control
    ValidFromDate Date,
    ValidToDate Nullable(Date),
    IsCurrent UInt8,
    SourceUpdateDate Date,
    Version UInt64
) ENGINE = ReplacingMergeTree(Version)
PARTITION BY toYYYYMM(ValidFromDate)
ORDER BY (EmployeeID, ValidFromDate);
```

#### 5. DimVendor

**Purpose**: Vendor master data with history.

**SCD Type**: Type 2.

```sql
CREATE TABLE DimVendor (
    -- Keys
    VendorKey UInt64,
    VendorID UInt32,
    
    -- Attributes
    VendorName String,
    ContactPerson String,
    Email String,
    Phone String,
    Address String,
    City LowCardinality(String),
    Country LowCardinality(String),
    VendorRating Decimal(3,2),
    OnTimeDeliveryRate Decimal(5,2),
    QualityScore Decimal(5,2),
    PaymentTerms String,
    VendorStatus LowCardinality(String),
    
    -- Version Control
    ValidFromDate Date,
    ValidToDate Nullable(Date),
    IsCurrent UInt8,
    SourceUpdateDate Date
) ENGINE = ReplacingMergeTree(IsCurrent)
PARTITION BY toYYYYMM(ValidFromDate)
ORDER BY (VendorID, ValidFromDate);
```

#### 6. DimWarehouse

**Purpose**: Warehouse location data.

**SCD Type**: Type 2.

```sql
CREATE TABLE DimWarehouse (
    -- Keys
    WarehouseKey UInt64,
    WarehouseID UInt32,
    
    -- Attributes
    WarehouseName String,
    Location String,
    WarehouseType LowCardinality(String),
    ManagerKey UInt64,
    
    -- Version Control
    ValidFromDate Date,
    ValidToDate Nullable(Date),
    IsCurrent UInt8
) ENGINE = ReplacingMergeTree(IsCurrent)
PARTITION BY toYYYYMM(ValidFromDate)
ORDER BY (WarehouseKey, WarehouseID);
```

### SCD Type 1 Dimensions (No History)

#### 7. DimDate

**Purpose**: Calendar dimension for time-based analysis.

**SCD Type**: Type 1 (dates never change).

```sql
CREATE TABLE DimDate (
    -- Key
    DateKey Date,
    
    -- Date Attributes
    Year UInt16,
    Quarter UInt8,
    Month UInt8,
    MonthName LowCardinality(String),
    Week UInt8,
    DayOfWeek UInt8,
    DayName LowCardinality(String),
    DayOfMonth UInt8,
    DayOfYear UInt16,
    WeekOfYear UInt8,
    
    -- Flags
    IsWeekend UInt8,
    IsHoliday UInt8,
    
    -- Descriptors
    HolidayName LowCardinality(String),
    FiscalYear UInt16,
    FiscalQuarter UInt8,
    FiscalMonth UInt8,
    Season LowCardinality(String)
) ENGINE = MergeTree()
PRIMARY KEY DateKey
ORDER BY DateKey;
```

**Example Rows**:
```
DateKey     | Year | Month | MonthName | DayName  | IsWeekend | IsHoliday | HolidayName
2025-12-25  | 2025 | 12    | December  | Thursday | 0         | 1         | Christmas
2025-12-28  | 2025 | 12    | December  | Sunday   | 1         | 0         | 
```

### Static Lookup Dimensions

#### 8. DimProductCategory

```sql
CREATE TABLE DimProductCategory (
    ProductCategoryKey UInt64,
    ProductCategoryID UInt32,
    CategoryName String,
    CategoryDescription String
) ENGINE = MergeTree()
PRIMARY KEY (ProductCategoryKey, ProductCategoryID)
ORDER BY (ProductCategoryKey, ProductCategoryID);
```

#### 9. DimSalesTerritory

```sql
CREATE TABLE DimSalesTerritory (
    TerritoryKey UInt64,
    TerritoryID UInt32,
    TerritoryName String,
    SalesRegion LowCardinality(String),
    Country LowCardinality(String),
    Manager String,
    SalesTarget Decimal(18, 2)
) ENGINE = MergeTree()
PRIMARY KEY (TerritoryKey, TerritoryID)
ORDER BY (TerritoryKey, TerritoryID);
```

#### 10. DimPromotion

```sql
CREATE TABLE DimPromotion (
    PromotionKey UInt64,
    PromotionID UInt32,
    PromotionName String,
    PromotionDescription String,
    PromotionType LowCardinality(String),
    DiscountPercentage Decimal(5, 2),
    DiscountAmount Decimal(18, 2),
    PromotionStatus LowCardinality(String),
    TargetCustomerSegment Nullable(String),
    CampaignID UInt32,
    TargetProductKey Nullable(UInt64),
    StartDate Date,
    EndDate Nullable(Date),
    IsActive UInt8
) ENGINE = ReplacingMergeTree(IsActive)
PRIMARY KEY (PromotionKey, PromotionID)
ORDER BY (PromotionKey, PromotionID);
```

#### 11-16. Small Lookup Tables

```sql
-- Feedback categories
CREATE TABLE DimFeedbackCategory (
    FeedbackCategoryKey UInt64,
    FeedbackCategoryID UInt32,
    CategoryName String,
    CategoryDescription String
) ENGINE = MergeTree()
PRIMARY KEY (FeedbackCategoryKey, FeedbackCategoryID)
ORDER BY (FeedbackCategoryKey, FeedbackCategoryID);

-- Return reasons
CREATE TABLE DimReturnReason (
    ReturnReasonKey UInt64,
    ReturnReasonID UInt32,
    ReturnReasonName LowCardinality(String),
    ReturnReasonDescription String
) ENGINE = MergeTree()
PRIMARY KEY ReturnReasonKey
ORDER BY (ReturnReasonKey, ReturnReasonID);

-- Customer segments
CREATE TABLE DimCustomerSegment (
    SegmentKey UInt64,
    SegmentID UInt32,
    SegmentName LowCardinality(String),
    SegmentDescription String,
    DiscountTierStart Decimal(5, 2),
    DiscountTierEnd Decimal(5, 2)
) ENGINE = MergeTree()
PRIMARY KEY (SegmentKey, SegmentID)
ORDER BY (SegmentKey, SegmentID);

-- Aging tiers (for inventory)
CREATE TABLE DimAgingTier (
    AgingTierKey UInt64,
    AgingTierID UInt32,
    AgingTierName LowCardinality(String),
    MinAgingDays UInt32,
    MaxAgingDays UInt32
) ENGINE = MergeTree()
PRIMARY KEY AgingTierKey
ORDER BY AgingTierKey;

-- Finance categories
CREATE TABLE DimFinanceCategory (
    FinanceCategoryKey UInt64,
    FinanceCategoryID UInt32,
    CategoryName LowCardinality(String),
    CategoryDescription String
) ENGINE = MergeTree()
PRIMARY KEY (FinanceCategoryKey, FinanceCategoryID)
ORDER BY (FinanceCategoryKey, FinanceCategoryID);

-- Regions
CREATE TABLE DimRegion (
    RegionKey UInt64,
    RegionID UInt32,
    RegionName String,
    Country LowCardinality(String),
    Continent LowCardinality(String),
    TimeZone LowCardinality(String)
) ENGINE = MergeTree()
PRIMARY KEY (RegionKey, RegionID)
ORDER BY (RegionKey, RegionID);
```

---

## Grain Definitions

### What is Grain?

**Grain** = The level of detail stored in a fact table.

```
Example: FactSales grain = One row per order line item

Order 12345:
  Line 1: Product A, Qty 2
  Line 2: Product B, Qty 1
  Line 3: Product C, Qty 3

FactSales rows: 3 (one per line)
```

### Fact Table Grains

| Fact Table | Grain | Example |
|------------|-------|---------|
| FactSales | Order line item | Order 12345, Line 3 |
| FactPurchases | Purchase order line | PO 456, Line 2 |
| FactInventory | Date + Product + Store + Warehouse | Nov 11, Product 789, Store 5, Warehouse 3 |
| FactProduction | Production run | Run ID 999 |
| FactEmployeeSales | Date + Employee + Store + SalesTerritory | Nov 11, Employee 50, Store 5, Territory 3 |
| FactCustomerFeedback | Feedback submission | Feedback ID 1234 |
| FactPromotionResponse | Date + Product + Store + Promotion | Nov 11, Product 789, Store 5, Promo 10 |
| FactFinance | Invoice | Invoice 12345 |
| FactReturns | Return transaction | Return ID RET-12345-1 |

### Why Grain Matters

**Query Performance**:
```sql
-- Fine grain (line item): 274K rows for 1 day
SELECT * FROM FactSales WHERE SalesDateKey = '2025-11-11';

-- Coarse grain (daily total): 1 row for 1 day
SELECT * FROM agg_daily_sales WHERE SalesDateKey = '2025-11-11';
```

**Storage**:
```
Fine grain:   274K rows/day × 365 days = 100M rows/year
Coarse grain: 30 rows/day × 365 days = 11K rows/year (9,000x smaller)
```

**Trade-off**: Fine grain = flexible but large; Coarse grain = fast but limited.

---

## Key Relationships

### Primary Relationships

```
DimCustomer (CustomerKey) ←─── FactSales (CustomerKey)
DimProduct (ProductKey) ←───── FactSales (ProductKey)
DimStore (StoreKey) ←────────── FactSales (StoreKey)
DimEmployee (EmployeeKey) ←─── FactSales (EmployeeKey)
DimDate (DateKey) ←──────────── FactSales (SalesDateKey)
```

### Referential Integrity

**Enforced**:
- All foreign keys must exist in dimension tables
- FK misses logged to error_records

**Example FK Resolution**:
```python
# Lookup CustomerKey from DimCustomer
SELECT CustomerKey FROM DimCustomer 
WHERE CustomerID = 500 AND IsCurrent = 1;

# If found: Use CustomerKey in FactSales
# If not found: Log error, skip row
```

### Conforming Dimensions

**Shared dimensions** used across multiple facts:

```
DimProduct ←── FactSales
           ←── FactPurchases
           ←── FactInventory
           ←── FactProduction
           ←── FactPromotionResponse
```

Benefit: Consistent product data across all analyses.

### Degenerate Dimensions

**Degenerate dimension** = Dimension attribute stored in fact table (no separate dimension table).

Example in FactSales:
- UnitPrice
- UnitPriceDiscount
- LineTotal
- InvoiceNumber (in FactFinance)

Why? Too many unique values, not worth separate dimension.

---

## Summary

**Schema Design**:
- **Star schema** with 9 fact tables and 16 dimension tables
- **SCD Type 2** for Customer, Product, Store, Employee, Vendor, Warehouse (history tracked)
- **SCD Type 1** for Date and static lookups (no history)

**Key Features**:
- **Conforming dimensions** (Product shared across facts)
- **Clearly defined grain** for each fact table
- **Referential integrity** enforced via ETL error handling
- **Degenerate dimensions** for high-cardinality attributes

**Next Steps**:
- See [ClickHouse Physical Design](clickhouse_physical_design.md) for implementation details
- See [Transformation Logic](transformation_logic.md) for SCD Type 2 merge process
- See [Data Mapping](data_mapping.md) for source-to-target mappings