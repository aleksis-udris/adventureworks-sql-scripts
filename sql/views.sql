-- Create Views For The Warehouse

-- Create Date Dimension Refreshable View
CREATE MATERIALIZED VIEW IF NOT EXISTS ADVENTUREWORKS_DWS.DimDate_MV
REFRESH EVERY 1 DAY
TO ADVENTUREWORKS_DWS.DimDate
AS
SELECT
    date_value AS DateKey,
    toYear(date_value) AS Year,
    toQuarter(date_value) AS Quarter,
    toMonth(date_value) AS Month,
    dateName('month', date_value) AS MonthName,
    toWeek(date_value, 3) AS Week,
    toDayOfWeek(date_value) AS DayOfWeek,
    dateName('weekday', date_value) AS DayName,
    toDayOfMonth(date_value) AS DayOfMonth,
    toDayOfYear(date_value) AS DayOfYear,
    toISOWeek(date_value) AS WeekOfYear,

    CASE WHEN toDayOfWeek(date_value) IN (6, 7) THEN 1 ELSE 0 END AS IsWeekend,

    CASE
        WHEN toMonth(date_value) = 1 AND toDayOfMonth(date_value) = 1 THEN 1
        WHEN toMonth(date_value) = 1 AND toDayOfWeek(date_value) = 1 AND toDayOfMonth(date_value) BETWEEN 15 AND 21 THEN 1
        WHEN toMonth(date_value) = 2 AND toDayOfWeek(date_value) = 1 AND toDayOfMonth(date_value) BETWEEN 15 AND 21 THEN 1
        WHEN toMonth(date_value) = 5 AND toDayOfWeek(date_value) = 1 AND toDayOfMonth(date_value) >= 25 THEN 1
        WHEN toMonth(date_value) = 7 AND toDayOfMonth(date_value) = 4 THEN 1
        WHEN toMonth(date_value) = 9 AND toDayOfWeek(date_value) = 1 AND toDayOfMonth(date_value) BETWEEN 1 AND 7 THEN 1
        WHEN toMonth(date_value) = 10 AND toDayOfWeek(date_value) = 1 AND toDayOfMonth(date_value) BETWEEN 8 AND 14 THEN 1
        WHEN toMonth(date_value) = 11 AND toDayOfMonth(date_value) = 11 THEN 1
        WHEN toMonth(date_value) = 11 AND toDayOfWeek(date_value) = 4 AND toDayOfMonth(date_value) BETWEEN 22 AND 28 THEN 1
        WHEN toMonth(date_value) = 12 AND toDayOfMonth(date_value) = 25 THEN 1
        ELSE 0
    END AS IsHoliday,

    CASE
        WHEN toMonth(date_value) = 1 AND toDayOfMonth(date_value) = 1 THEN 'New Year''s Day'
        WHEN toMonth(date_value) = 1 AND toDayOfWeek(date_value) = 1 AND toDayOfMonth(date_value) BETWEEN 15 AND 21 THEN 'Martin Luther King Jr. Day'
        WHEN toMonth(date_value) = 2 AND toDayOfWeek(date_value) = 1 AND toDayOfMonth(date_value) BETWEEN 15 AND 21 THEN 'Presidents'' Day'
        WHEN toMonth(date_value) = 5 AND toDayOfWeek(date_value) = 1 AND toDayOfMonth(date_value) >= 25 THEN 'Memorial Day'
        WHEN toMonth(date_value) = 7 AND toDayOfMonth(date_value) = 4 THEN 'Independence Day'
        WHEN toMonth(date_value) = 9 AND toDayOfWeek(date_value) = 1 AND toDayOfMonth(date_value) BETWEEN 1 AND 7 THEN 'Labor Day'
        WHEN toMonth(date_value) = 10 AND toDayOfWeek(date_value) = 1 AND toDayOfMonth(date_value) BETWEEN 8 AND 14 THEN 'Columbus Day'
        WHEN toMonth(date_value) = 11 AND toDayOfMonth(date_value) = 11 THEN 'Veterans Day'
        WHEN toMonth(date_value) = 11 AND toDayOfWeek(date_value) = 4 AND toDayOfMonth(date_value) BETWEEN 22 AND 28 THEN 'Thanksgiving'
        WHEN toMonth(date_value) = 12 AND toDayOfMonth(date_value) = 25 THEN 'Christmas Day'
        ELSE ''
    END AS HolidayName,

    CASE WHEN toMonth(date_value) >= 7 THEN toYear(date_value) + 1 ELSE toYear(date_value) END AS FiscalYear,

    CASE
        WHEN toMonth(date_value) IN (7, 8, 9) THEN 1
        WHEN toMonth(date_value) IN (10, 11, 12) THEN 2
        WHEN toMonth(date_value) IN (1, 2, 3) THEN 3
        ELSE 4
    END AS FiscalQuarter,

    CASE WHEN toMonth(date_value) >= 7 THEN toMonth(date_value) - 6 ELSE toMonth(date_value) + 6 END AS FiscalMonth,

    CASE
        WHEN toMonth(date_value) IN (12, 1, 2) THEN 'Winter'
        WHEN toMonth(date_value) IN (3, 4, 5) THEN 'Spring'
        WHEN toMonth(date_value) IN (6, 7, 8) THEN 'Summer'
        ELSE 'Fall'
    END AS Season

FROM (
    -- Generate dates from 2011-01-01 to 10 years from today
    SELECT toDate('2011-01-01') + number AS date_value
    FROM numbers(dateDiff('day', toDate('2011-01-01'), today() + INTERVAL 10 YEAR) + 1)
);

-- Manual refresh if needed
SYSTEM REFRESH VIEW ADVENTUREWORKS_DWS.DimDate_MV;

-- Check refresh status
SELECT * FROM system.view_refreshes
WHERE view = 'DimDate_MV';

-- Create Materialized View For Daily Sales
DROP VIEW IF EXISTS ADVENTUREWORKS_DS.mv_agg_daily_sales;
CREATE MATERIALIZED VIEW ADVENTUREWORKS_DS.mv_agg_daily_sales
TO ADVENTUREWORKS_DS.agg_daily_sales
AS
SELECT
    fs.SalesDateKey,
    fs.StoreKey,
    COALESCE(dpc.ProductCategoryKey, 0) AS ProductCategoryKey,
    sum(fs.SalesRevenue)      AS RevenueSum,
    sum(fs.QuantitySold)      AS QuantitySum,
    sum(fs.DiscountAmount)    AS DiscountSum,
    sum(fs.NumberOfTransactions) AS TransactionCount
FROM ADVENTUREWORKS_DWS.FactSales AS fs
LEFT JOIN ADVENTUREWORKS_DWS.DimProduct AS dp
    ON fs.ProductKey = dp.ProductKey
LEFT JOIN ADVENTUREWORKS_DWS.DimProductCategory AS dpc
    ON dpc.CategoryName = dp.Category
GROUP BY
    fs.SalesDateKey, fs.StoreKey, ProductCategoryKey;

-- Create Materialized View for Weekly Sales
DROP VIEW IF EXISTS ADVENTUREWORKS_DS.mv_agg_weekly_sales;
CREATE MATERIALIZED VIEW ADVENTUREWORKS_DS.mv_agg_weekly_sales
TO ADVENTUREWORKS_DS.agg_weekly_sales
AS
SELECT
    dateTrunc('week', fs.SalesDateKey) AS WeekStartDateKey,
    COALESCE(dr.RegionKey, 0)          AS RegionKey,
    COALESCE(dpc.ProductCategoryKey, 0) AS ProductCategoryKey,
    sum(fs.SalesRevenue)               AS RevenueSum,
    avg(fs.SalesRevenue)               AS RevenueAvg,
    min(fs.SalesRevenue)               AS RevenueMin,
    max(fs.SalesRevenue)               AS RevenueMax,
    sum(fs.QuantitySold)               AS QuantitySum
FROM ADVENTUREWORKS_DWS.FactSales AS fs
LEFT JOIN ADVENTUREWORKS_DWS.DimStore AS ds
    ON fs.StoreKey = ds.StoreKey
LEFT JOIN ADVENTUREWORKS_DWS.DimRegion AS dr
    ON dr.RegionName = ds.Region
LEFT JOIN ADVENTUREWORKS_DWS.DimProduct AS dp
    ON fs.ProductKey = dp.ProductKey
LEFT JOIN ADVENTUREWORKS_DWS.DimProductCategory AS dpc
    ON dpc.CategoryName = dp.Category
GROUP BY WeekStartDateKey, RegionKey, ProductCategoryKey;

-- Create Materialized View for Monthly Sales
DROP VIEW IF EXISTS ADVENTUREWORKS_DS.mv_agg_monthly_sales;
CREATE MATERIALIZED VIEW ADVENTUREWORKS_DS.mv_agg_monthly_sales
TO ADVENTUREWORKS_DS.agg_monthly_sales
AS
SELECT
    dateTrunc('month', fs.SalesDateKey) AS MonthStartDateKey,
    COALESCE(dcs.SegmentKey, 0)         AS CustomerSegmentKey,
    COALESCE(dr.RegionKey, 0)           AS RegionKey,
    sum(fs.SalesRevenue)                AS RevenueSum,
    avg(fs.SalesRevenue)                AS AvgOrderValue,
    countDistinct(fs.CustomerKey)       AS DistinctCustomers
FROM ADVENTUREWORKS_DWS.FactSales AS fs
LEFT JOIN ADVENTUREWORKS_DWS.DimCustomer AS dc
    ON fs.CustomerKey = dc.CustomerKey
LEFT JOIN ADVENTUREWORKS_DWS.DimCustomerSegment AS dcs
    ON dcs.SegmentName = dc.CustomerSegment
LEFT JOIN ADVENTUREWORKS_DWS.DimStore AS ds
    ON fs.StoreKey = ds.StoreKey
LEFT JOIN ADVENTUREWORKS_DWS.DimRegion AS dr
    ON dr.RegionName = ds.Region
GROUP BY MonthStartDateKey, CustomerSegmentKey, RegionKey;

-- Create Materialized View for Daily Inventory
DROP VIEW IF EXISTS ADVENTUREWORKS_DS.mv_agg_daily_inventory;
CREATE MATERIALIZED VIEW ADVENTUREWORKS_DS.mv_agg_daily_inventory
TO ADVENTUREWORKS_DS.agg_daily_inventory
AS
SELECT
    fi.InventoryDateKey                                         AS InventoryDateKey,
    fi.WarehouseKey                                             AS WarehouseKey,
    COALESCE(dpc.ProductCategoryKey, 0)                         AS ProductCategoryKey,
    COALESCE(dat.AgingTierKey, 0)                               AS AgingTierKey,
    AVG(fi.QuantityOnHand * COALESCE(dp.Cost, dp.ListPrice, 0)) AS AvgInventoryValue,
    AVG(fi.QuantityOnHand)                                       AS AvgQuantityOnHand
FROM ADVENTUREWORKS_DWS.FactInventory         AS fi
LEFT JOIN ADVENTUREWORKS_DWS.DimProduct       AS dp
       ON fi.ProductKey = dp.ProductKey
LEFT JOIN ADVENTUREWORKS_DWS.DimProductCategory AS dpc
       ON dpc.CategoryName = dp.Category
LEFT JOIN ADVENTUREWORKS_DWS.DimAgingTier     AS dat
       ON fi.StockAging BETWEEN dat.MinAgingDays AND dat.MaxAgingDays
GROUP BY
    InventoryDateKey, WarehouseKey, ProductCategoryKey, AgingTierKey;

-- Create Materialized View for Monthly Product Performance
DROP VIEW IF EXISTS ADVENTUREWORKS_DS.mv_agg_monthly_product_performance;
CREATE MATERIALIZED VIEW ADVENTUREWORKS_DS.mv_agg_monthly_product_performance
TO ADVENTUREWORKS_DS.agg_monthly_product_performance
AS
WITH sales AS (
    SELECT
        dateTrunc('month', fs.SalesDateKey) AS mth,
        fs.ProductKey,
        fs.StoreKey,
        sum(fs.SalesRevenue) AS rev,
        sum(fs.QuantitySold) AS qty
    FROM ADVENTUREWORKS_DWS.FactSales fs
    GROUP BY mth, fs.ProductKey, fs.StoreKey
),
returns AS (
    SELECT
        dateTrunc('month', fr.ReturnDateKey) AS mth,
        fr.ProductKey,
        fr.StoreKey,
        sum(fr.ReturnedQuantity) AS r_qty
    FROM ADVENTUREWORKS_DWS.FactReturns fr
    GROUP BY mth, fr.ProductKey, fr.StoreKey
)
SELECT
    s.mth AS MonthStartDateKey,
    s.ProductKey,
    s.StoreKey,
    s.rev AS RevenueSum,
    s.qty AS UnitsSold,
    COALESCE(r.r_qty, 0) AS ReturnsQty,
    CAST( if(s.qty = 0, 0, (COALESCE(r.r_qty, 0) / s.qty) * 100) AS Decimal(10,4) ) AS ReturnsRatePct,
    CAST(NULL AS Nullable(Decimal(5,2))) AS AvgRating
FROM sales s
LEFT JOIN returns r
    ON r.mth = s.mth AND r.ProductKey = s.ProductKey AND r.StoreKey = s.StoreKey;

-- Create Materialized View for Regional Sales
DROP VIEW IF EXISTS ADVENTUREWORKS_DS.mv_agg_regional_sales;
CREATE MATERIALIZED VIEW ADVENTUREWORKS_DS.mv_agg_regional_sales
TO ADVENTUREWORKS_DS.agg_regional_sales
AS
WITH base AS (
    SELECT
        dateTrunc('month', fs.SalesDateKey) AS mth,
        COALESCE(dr.RegionKey, 0)           AS RegionKey,
        COALESCE(dst.TerritoryKey, 0)       AS SalesTerritoryKey,
        sum(fs.SalesRevenue)                AS RevenueSum
    FROM ADVENTUREWORKS_DWS.FactSales AS fs
    LEFT JOIN ADVENTUREWORKS_DWS.DimStore AS ds
        ON fs.StoreKey = ds.StoreKey
    LEFT JOIN ADVENTUREWORKS_DWS.DimRegion AS dr
        ON dr.RegionName = ds.Region
    LEFT JOIN ADVENTUREWORKS_DWS.DimSalesTerritory AS dst
        ON dst.TerritoryName = ds.Territory -- name-based bridge
    GROUP BY mth, RegionKey, SalesTerritoryKey
),
lagged AS (
    SELECT
        mth,
        RegionKey,
        SalesTerritoryKey,
        RevenueSum,
        anyLast(RevenueSum) OVER (
            PARTITION BY RegionKey, SalesTerritoryKey
            ORDER BY mth
            ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING
        ) AS PrevRevenue
    FROM base
)
SELECT
    mth AS MonthStartDateKey,
    RegionKey,
    SalesTerritoryKey,
    RevenueSum,
    CAST(ifNull( if(PrevRevenue = 0 OR PrevRevenue IS NULL, 0, (RevenueSum - PrevRevenue) / PrevRevenue * 100 ), 0) AS Decimal(10,4)) AS GrowthRatePct
FROM lagged;