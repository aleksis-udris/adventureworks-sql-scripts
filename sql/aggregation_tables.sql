-- Create Data Aggregation Database
CREATE DATABASE IF NOT EXISTS ADVENTUREWORKS_DS;

-- Data Aggregations

-- Daily Sales Aggregation
-- Update Frequency: Daily
-- Garin: One row per Store per Product Category per Day
CREATE TABLE IF NOT EXISTS ADVENTUREWORKS_DS.AggDailySales (
    -- Foreign Keys
    SalesDate Date,
    StoreKey UInt32,
    ProductCategoryKey UInt32,

    -- Metrics
    TotalSalesRevenue Decimal(18, 2),
    TotalQuantitySold UInt64,
    TotalDiscountAmount Decimal(18, 2),
    TransactionCount UInt32,
) ENGINE = MergeTree
PARTITION BY toYYYYMM(SalesDate)
ORDER BY (SalesDate, StoreKey, ProductCategoryKey);

-- Weekly Sales Aggregation
-- Update Frequency: Weekly (Sundays)
-- Grain: One row per Region per Product Category per Week
CREATE TABLE IF NOT EXISTS ADVENTUREWORKS_DS.AggWeeklySales (
    -- Foreign Keys
    WeekStartDate Date,
    RegionKey UInt64,
    ProductCategoryKey UInt64,

    -- Metrics
    WeeklySalesTotal Decimal(18, 2),
    WeeklySalesPerDayAvg Decimal(18, 2),
    SmallestSale Decimal(18, 2),
    BiggestSale Decimal(18, 2)
) ENGINE = MergeTree() PARTITION BY toYYYYMM(WeekStartDate)
ORDER BY (WeekStartDate, RegionKey, ProductCategoryKey);

-- Monthly Sale Aggregation
-- Update Frequency: Monthly (1st of next month)
-- Grain: One row per Customer Segment per Region per Month
CREATE TABLE IF NOT EXISTS ADVENTUREWORKS_DS.AggMonthlySales (
    -- Foreign Keys
    MonthStartDate Date,
    CustomerSegmentKey UInt64,
    RegionKey UInt64,

    -- Metrics
    TotalRevenue Decimal(18, 2),
    AvgOrderValue Decimal(18, 2),
    DistinctCustomerCount UInt32,
) ENGINE = MergeTree() PARTITION BY toYYYYMM(MonthStartDate)
ORDER BY (MonthStartDate, CustomerSegmentKey, RegionKey);

-- Daily Inventory Aggregation
-- Update Frequency: Daily
-- Grain: One row per Warehouse per Product Category per Aging Tier per Day
CREATE TABLE IF NOT EXISTS ADVENTUREWORKS_DS.AggDailyInventory (

    -- Foreign Keys
    InventoryDate Date,
    WarehouseKey UInt64,
    ProductCategory UInt64,
    AgingTierKey UInt64,

    -- Metrics
    TotalInventoryValue Decimal(18, 2)
    WarehouseInventoryValue Decimal(18, 2),
    ProductCategoryValue Decimal(18, 2),
    AgingTierValue Decimal(18, 2)
) ENGINE = MergeTree() PARTITION BY toYYYYMM(InventoryDate)
ORDER BY (InventoryDate, WarehouseKey, ProductCategory, AgingTier);

-- Monthly Product Performance Aggregation
-- Update Frequency: Monthly
-- Grain: One row per Product per Store per Month
CREATE TABLE IF NOT EXISTS ADVENTUREWORKS_DS.AggMonthlyProductPerformance (

    -- Foreign Keys
    MonthStartDate Date,
    ProductKey UInt64,
    StoreKey UInt64,

    -- Metrics
    TotalRevenue Decimal(18, 2),
    UnitsSold UInt64,
    ReturnRate Decimal(18, 2),
    AvgRating Decimal(5, 2), -- Between 1.00 and 5.00
) ENGINE = MergeTree() PARTITION BY toYYYYMM(MonthStartDate)
ORDER BY (MonthStartDate, ProductKey, StoreKey);

-- Regional Sales Aggregation
-- Update Frequency: Monthly
-- Grain: One row per Region per Territory per Month
CREATE TABLE agg_regional_sales
(
    -- Foreign Keys
    MonthStartDate Date,
    RegionKey UInt64,
    SalesTerritoryKey UInt64,

    -- Metrics: General
    TotalSalesRevenue Decimal(18, 2),
    TotalQuantitySold UInt64,
    TotalDiscountAmount Decimal(18, 2),
    TransactionCount UInt64,

    --Metrics: Growth
    RevenueMoMGrowth Decimal(18, 2),
    QuantityMoMGrowth Decimal(18, 2)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(MonthStartDate)
ORDER BY (MonthStartDate, RegionKey, SalesTerritoryKey);