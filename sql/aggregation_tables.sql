-- Aggregations: ADVENTUREWORKS DATA STORE
-- Create Database ADVENTUREWORKS_DS (if it doesn't exist)
CREATE DATABASE IF NOT EXISTS ADVENTUREWORKS_DS;

-- Create Daily Sale Aggregation | Update Frequency: Daily
DROP TABLE IF EXISTS ADVENTUREWORKS_DS.agg_daily_sales;
CREATE TABLE ADVENTUREWORKS_DS.agg_daily_sales
(
    SalesDateKey          Date,
    StoreKey              UInt64,
    ProductCategoryKey    UInt64,

    RevenueSum            Decimal(18,2),
    QuantitySum           UInt64,
    DiscountSum           Decimal(18,2),
    TransactionCount      UInt64
)
ENGINE = MergeTree
PARTITION BY SalesDateKey
ORDER BY (SalesDateKey, StoreKey, ProductCategoryKey);

-- Create Aggregation Weekly Sales | Update Frequency: Weekly
DROP TABLE IF EXISTS ADVENTUREWORKS_DS.agg_weekly_sales;
CREATE TABLE ADVENTUREWORKS_DS.agg_weekly_sales
(
    WeekStartDateKey      Date,
    RegionKey             UInt64,
    ProductCategoryKey    UInt64,

    RevenueSum            Decimal(18,2),
    RevenueAvg            Decimal(18,2),
    RevenueMin            Decimal(18,2),
    RevenueMax            Decimal(18,2),
    QuantitySum           UInt64
)
ENGINE = MergeTree
PARTITION BY WeekStartDateKey
ORDER BY (WeekStartDateKey, RegionKey, ProductCategoryKey);

-- Create Aggregation Monthly Sales | Update Frequency: Monthly
DROP TABLE IF EXISTS ADVENTUREWORKS_DS.agg_monthly_sales;
CREATE TABLE ADVENTUREWORKS_DS.agg_monthly_sales
(
    MonthStartDateKey     Date,
    CustomerSegmentKey    UInt64,
    RegionKey             UInt64,

    RevenueSum            Decimal(18,2),
    AvgOrderValue         Decimal(18,2),
    DistinctCustomers     UInt64
)
ENGINE = MergeTree
PARTITION BY MonthStartDateKey
ORDER BY (MonthStartDateKey, CustomerSegmentKey, RegionKey);

-- Create Aggregation Daily Inventory | Update Frequency: Daily
DROP TABLE IF EXISTS ADVENTUREWORKS_DS.agg_daily_inventory;
CREATE TABLE ADVENTUREWORKS_DS.agg_daily_inventory
(
    InventoryDateKey      Date,
    WarehouseKey          UInt64,
    ProductCategoryKey    UInt64,
    AgingTierKey          UInt64,

    AvgInventoryValue     Decimal(18,2),
    AvgQuantityOnHand     Float64
)
ENGINE = MergeTree
PARTITION BY InventoryDateKey
ORDER BY (InventoryDateKey, WarehouseKey, ProductCategoryKey, AgingTierKey);

-- Create Aggregation Monthly Product Performance | Update Frequency: Monthly
DROP TABLE IF EXISTS ADVENTUREWORKS_DS.agg_monthly_product_performance;
CREATE TABLE ADVENTUREWORKS_DS.agg_monthly_product_performance
(
    MonthStartDateKey     Date,
    ProductKey            UInt64,
    StoreKey              UInt64,

    RevenueSum            Decimal(18,2),
    UnitsSold             UInt64,
    ReturnsQty            UInt64,
    ReturnsRatePct        Decimal(10,4),
    AvgRating             Nullable(Decimal(5,2))
)
ENGINE = MergeTree
PARTITION BY MonthStartDateKey
ORDER BY (MonthStartDateKey, ProductKey, StoreKey);

-- Create Aggregation Regional Sales | Update Frequency: Monthly
DROP TABLE IF EXISTS ADVENTUREWORKS_DS.agg_regional_sales;
CREATE TABLE ADVENTUREWORKS_DS.agg_regional_sales
(
    MonthStartDateKey     Date,
    RegionKey             UInt64,
    SalesTerritoryKey     UInt64,

    RevenueSum            Decimal(18,2),
    GrowthRatePct         Decimal(10,4)
)
ENGINE = MergeTree
PARTITION BY MonthStartDateKey
ORDER BY (MonthStartDateKey, RegionKey, SalesTerritoryKey);