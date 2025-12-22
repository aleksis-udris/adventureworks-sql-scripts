--View 1: Full Extended Sales
CREATE OR REPLACE VIEW ADVENTUREWORKS_DWS.vw_sales_extended_full AS
SELECT
    f.SalesDateKey,
    d.DateKey, d.Year, d.Quarter, d.Month, d.MonthName, d.Week, d.DayOfWeek, d.FiscalYear, d.Season,
    f.CustomerKey, c.CustomerName, c.City, c.StateProvince, c.Country, c.CustomerSegment, c.CustomerType, c.AccountStatus, c.CreditLimit,
    f.ProductKey, p.ProductName, p.SKU, p.Category, p.SubCategory, p.Brand, p.ListPrice, p.Cost, p.ProductStatus,
    f.StoreKey, s.StoreName, s.City as StoreCity, s.Region, s.Territory, s.StoreType, s.SquareFootage, s.StoreStatus,
    f.EmployeeKey, e.EmployeeName, e.JobTitle, e.Department, e.Region as EmployeeRegion, e.Territory as EmployeeTerritory, e.SalesQuota,
    f.SalesRevenue as SalesAmount,
    f.QuantitySold as Quantity,
    f.DiscountAmount,
    f.NumberOfTransactions as TransactionCount,
    round(f.SalesRevenue / greatest(f.QuantitySold, 1), 2) as UnitPrice,
    round((f.SalesRevenue - f.DiscountAmount) / greatest(f.SalesRevenue, 1) * 100, 2) as DiscountPercentage,
    f.SalesRevenue - f.DiscountAmount as NetRevenue,
    (f.SalesRevenue - f.DiscountAmount) - (f.QuantitySold * p.Cost) as GrossProfit,
    round(((f.SalesRevenue - f.DiscountAmount - (f.QuantitySold * p.Cost)) / greatest(f.SalesRevenue, 1) * 100), 2) as ProfitMarginPercent,
    CASE
        WHEN (f.SalesRevenue - f.DiscountAmount - (f.QuantitySold * p.Cost)) > (f.SalesRevenue - f.DiscountAmount) * 0.3 THEN 'High Margin'
        WHEN (f.SalesRevenue - f.DiscountAmount - (f.QuantitySold * p.Cost)) > (f.SalesRevenue - f.DiscountAmount) * 0.15 THEN 'Medium Margin'
        ELSE 'Low Margin'
    END as MarginCategory
FROM FactSales AS f
LEFT JOIN DimDate AS d ON f.SalesDateKey = d.DateKey
LEFT JOIN DimCustomer AS c ON f.CustomerKey = c.CustomerKey AND c.IsCurrent = 1
LEFT JOIN DimProduct AS p ON f.ProductKey = p.ProductKey AND p.IsCurrent = 1
LEFT JOIN DimStore AS s ON f.StoreKey = s.StoreKey AND s.IsCurrent = 1
LEFT JOIN DimEmployee AS e ON f.EmployeeKey = e.EmployeeKey AND e.IsCurrent = 1;

-- View 2: Sales By Customer
CREATE OR REPLACE VIEW vw_sales_by_customer AS
SELECT
    c.CustomerKey, c.CustomerID, c.CustomerName, c.City, c.StateProvince, c.Country, c.CustomerSegment, c.CustomerType, c.AccountStatus, c.CreditLimit, c.AnnualIncome, c.YearsSinceFirstPurchase,
    uniq(fs.SalesDateKey) as PurchaseDays,
    uniq(fs.StoreKey) as StoresVisited,
    uniq(fs.ProductKey) as UniqueProductsPurchased,
    uniq(fs.EmployeeKey) as EmployeesInteracted,
    sum(fs.QuantitySold) as TotalQuantityPurchased,
    sum(fs.SalesRevenue) as TotalRevenue,
    sum(fs.SalesRevenue - fs.DiscountAmount) as NetRevenue,
    sum(fs.SalesRevenue - fs.DiscountAmount - (fs.QuantitySold * p.Cost)) as TotalProfit,
    round((sum(fs.SalesRevenue - fs.DiscountAmount - (fs.QuantitySold * p.Cost)) / greatest(sum(fs.SalesRevenue), 1) * 100), 2) as AvgProfitMarginPercent,
    avg(fs.SalesRevenue) as AvgOrderValue,
    max(fs.SalesDateKey) as LastPurchaseDate,
    min(fs.SalesDateKey) as FirstPurchaseDate,
    round((sum(fs.DiscountAmount) / greatest(sum(fs.SalesRevenue), 1) * 100), 2) as AvgDiscountPercentage,
    count(*) as TotalTransactions
FROM DimCustomer AS c
LEFT JOIN FactSales AS fs ON c.CustomerKey = fs.CustomerKey
LEFT JOIN DimProduct AS p ON fs.ProductKey = p.ProductKey
WHERE c.IsCurrent = 1
GROUP BY c.CustomerKey, c.CustomerID, c.CustomerName, c.City, c.StateProvince, c.Country, c.CustomerSegment, c.CustomerType, c.AccountStatus, c.CreditLimit, c.AnnualIncome, c.YearsSinceFirstPurchase;

-- View 3: Sales By Product
CREATE OR REPLACE VIEW vw_sales_by_product AS
SELECT
    p.ProductKey, p.ProductID, p.ProductName, p.SKU, p.Category, p.SubCategory, p.Brand, p.ListPrice, p.Cost, p.ProductStatus, p.Color, p.Size,
    uniq(fs.SalesDateKey) as SalesDays,
    sum(fs.QuantitySold) as TotalUnitsSold,
    uniq(fs.StoreKey) as StoresOffering,
    uniq(fs.CustomerKey) as UniqueCustomers,
    sum(fs.SalesRevenue) as TotalRevenue,
    sum(fs.SalesRevenue - fs.DiscountAmount) as NetRevenue,
    sum(fs.SalesRevenue - fs.DiscountAmount - (fs.QuantitySold * p.Cost)) as GrossProfit,
    round((sum(fs.SalesRevenue - fs.DiscountAmount - (fs.QuantitySold * p.Cost)) / greatest(sum(fs.SalesRevenue), 1) * 100), 2) as ProfitMarginPercent,
    round((sum(fs.SalesRevenue) / greatest(uniq(fs.SalesDateKey), 1)), 2) as DailyAvenueRevenue,
    round((sum(fs.QuantitySold) / greatest(uniq(fs.SalesDateKey), 1)), 2) as DailyAverageUnits,
    round((sum(fs.DiscountAmount) / greatest(sum(fs.SalesRevenue), 1) * 100), 2) as AvgDiscountPercentage,
    round(avg(fs.SalesRevenue), 2) as AvgOrderValue
FROM DimProduct AS p
LEFT JOIN FactSales AS fs ON p.ProductKey = fs.ProductKey
WHERE p.IsCurrent = 1
GROUP BY p.ProductKey, p.ProductID, p.ProductName, p.SKU, p.Category, p.SubCategory, p.Brand, p.ListPrice, p.Cost, p.ProductStatus, p.Color, p.Size;

-- View 4: Sales By Store
CREATE OR REPLACE VIEW vw_sales_by_store AS
SELECT
    s.StoreKey, s.StoreID, s.StoreName, s.City, s.StateProvince, s.Region, s.Territory, s.StoreType, s.StoreStatus, s.SquareFootage, s.OpeningDate,
    uniq(fs.SalesDateKey) as ActiveSalesDays,
    uniq(fs.CustomerKey) as UniqueCustomers,
    uniq(fs.ProductKey) as ProductsOffered,
    uniq(fs.EmployeeKey) as ActiveEmployees,
    sum(fs.QuantitySold) as TotalUnitsSold,
    sum(fs.SalesRevenue) as TotalRevenue,
    sum(fs.SalesRevenue - fs.DiscountAmount) as NetRevenue,
    sum(fs.SalesRevenue - fs.DiscountAmount - (fs.QuantitySold * p.Cost)) as GrossProfit,
    round((sum(fs.SalesRevenue) / greatest(s.SquareFootage, 1)), 2) as SalesPerSqFt,
    round((sum(fs.SalesRevenue - fs.DiscountAmount) / greatest(s.SquareFootage, 1)), 2) as NetRevenuePerSqFt,
    avg(fs.SalesRevenue) as AvgTransactionValue,
    round((sum(fs.DiscountAmount) / greatest(sum(fs.SalesRevenue), 1) * 100), 2) as AvgDiscountPercentage,
    max(fs.SalesDateKey) as LastSalesDate
FROM DimStore AS s
LEFT JOIN FactSales AS fs ON s.StoreKey = fs.StoreKey
LEFT JOIN DimProduct AS p ON fs.ProductKey = p.ProductKey
WHERE s.IsCurrent = 1
GROUP BY s.StoreKey, s.StoreID, s.StoreName, s.City, s.StateProvince, s.Region, s.Territory, s.StoreType, s.StoreStatus, s.SquareFootage, s.OpeningDate;

-- View 5: Sales By Employee
CREATE OR REPLACE VIEW vw_sales_by_employee AS
SELECT
    e.EmployeeKey, e.EmployeeID, e.EmployeeName, e.JobTitle, e.Department, e.Region, e.Territory, e.SalesQuota, e.EmployeeStatus, e.HireDate,
    uniq(fs.SalesDateKey) as ActiveSalesDays,
    uniq(fs.CustomerKey) as CustomersServed,
    uniq(fs.ProductKey) as ProductsSold,
    uniq(fs.StoreKey) as StoresWorked,
    sum(fs.QuantitySold) as TotalUnitsSold,
    sum(fs.SalesRevenue) as TotalRevenue,
    sum(fs.SalesRevenue - fs.DiscountAmount) as NetRevenue,
    sum(fs.SalesRevenue - fs.DiscountAmount - (fs.QuantitySold * p.Cost)) as GrossProfit,
    round(((sum(fs.SalesRevenue - fs.DiscountAmount) / greatest(e.SalesQuota, 1)) * 100), 2) as QuotaAttainmentPercent,
    round((sum(fs.SalesRevenue - fs.DiscountAmount) / greatest(uniq(fs.SalesDateKey), 1)), 2) as DailyAverageRevenue,
    avg(fs.SalesRevenue) as AvgOrderValue,
    round((sum(fs.DiscountAmount) / greatest(sum(fs.SalesRevenue), 1) * 100), 2) as AvgDiscountPercentage,
    max(fs.SalesDateKey) as LastSalesDate
FROM DimEmployee AS e
LEFT JOIN FactSales AS fs ON e.EmployeeKey = fs.EmployeeKey
LEFT JOIN DimProduct AS p ON fs.ProductKey = p.ProductKey
WHERE e.IsCurrent = 1
GROUP BY e.EmployeeKey, e.EmployeeID, e.EmployeeName, e.JobTitle, e.Department, e.Region, e.Territory, e.SalesQuota, e.EmployeeStatus, e.HireDate;

-- View 6: Inventory Snapshot
CREATE OR REPLACE VIEW vw_inventory_snapshot AS
SELECT
    i.InventoryDateKey, d.DateKey, d.Year, d.Month, d.MonthName,
    i.ProductKey, p.ProductName, p.SKU, p.Category, p.SubCategory, p.ListPrice, p.Cost,
    i.StoreKey, s.StoreName, s.Region, s.Territory,
    i.QuantityOnHand,
    round((i.QuantityOnHand * p.ListPrice), 2) as InventoryValueAtListPrice,
    round((i.QuantityOnHand * p.Cost), 2) as InventoryValueAtCost,
    i.ReorderLevel, i.SafetyStockLevels, i.StockAging,
    CASE
        WHEN i.StockAging <= 30 THEN 'Fresh (0-30 days)'
        WHEN i.StockAging <= 90 THEN 'Aged (31-90 days)'
        WHEN i.StockAging <= 180 THEN 'Very Aged (91-180 days)'
        ELSE 'Obsolete (180+ days)'
    END as AgingCategory,
    CASE
        WHEN i.QuantityOnHand <= i.ReorderLevel THEN 'Critical'
        WHEN i.QuantityOnHand <= i.SafetyStockLevels THEN 'Low'
        WHEN i.QuantityOnHand >= (i.ReorderLevel * 3) THEN 'Overstock'
        ELSE 'Normal'
    END as StockStatus,
    round(greatest(0, i.QuantityOnHand - i.ReorderLevel), 0) as ExcessQuantity,
    round(greatest(0, i.ReorderLevel - i.QuantityOnHand), 0) as ShortageQuantity
FROM FactInventory AS i
LEFT JOIN DimDate AS d ON i.InventoryDateKey = d.DateKey
LEFT JOIN DimProduct AS p ON i.ProductKey = p.ProductKey AND p.IsCurrent = 1
LEFT JOIN DimStore AS s ON i.StoreKey = s.StoreKey AND s.IsCurrent = 1;

-- View 7: Inventory By Product
CREATE OR REPLACE VIEW vw_inventory_by_product AS
SELECT
    p.ProductKey, p.ProductID, p.ProductName, p.SKU, p.Category, p.SubCategory, p.ListPrice, p.Cost, p.ProductStatus,
    sum(i.QuantityOnHand) as TotalQuantityOnHand,
    uniq(i.StoreKey) as StoresStockingProduct,
    round((sum(i.QuantityOnHand * p.ListPrice)), 2) as TotalInventoryValueAtListPrice,
    round((sum(i.QuantityOnHand * p.Cost)), 2) as TotalInventoryValueAtCost,
    round(avg(i.StockAging), 2) as AvgStockAgingDays,
    max(i.StockAging) as MaxStockAgingDays,
    sumIf(1, i.QuantityOnHand <= i.ReorderLevel) as LocationsBelowReorderLevel,
    sumIf(1, i.QuantityOnHand <= i.SafetyStockLevels) as LocationsAtRiskLevel,
    sumIf(1, i.QuantityOnHand >= (i.ReorderLevel * 3)) as OverstockedLocations,
    round((sum(greatest(0, i.QuantityOnHand - i.ReorderLevel)) / greatest(sum(i.QuantityOnHand), 1) * 100), 2) as ExcessPercentage
FROM DimProduct AS p
LEFT JOIN FactInventory AS i ON p.ProductKey = i.ProductKey
WHERE p.IsCurrent = 1
GROUP BY p.ProductKey, p.ProductID, p.ProductName, p.SKU, p.Category, p.SubCategory, p.ListPrice, p.Cost, p.ProductStatus;

-- View 8: Inventory By Warehouse
CREATE OR REPLACE VIEW vw_inventory_by_warehouse AS
SELECT
    w.WarehouseKey, w.WarehouseID, w.WarehouseName, w.Location, w.WarehouseType,
    sum(i.QuantityOnHand) as TotalQuantityOnHand,
    uniq(i.ProductKey) as UniqueProductsStored,
    uniq(i.StoreKey) as ServicedLocations,
    round((sum(i.QuantityOnHand * p.ListPrice)), 2) as TotalInventoryValueAtListPrice,
    round((sum(i.QuantityOnHand * p.Cost)), 2) as TotalInventoryValueAtCost,
    countIf(i.QuantityOnHand <= i.ReorderLevel) as CriticalStockItems,
    round(avg(i.StockAging), 1) as AvgStockAgingDays,
    sumIf(i.QuantityOnHand, i.StockAging > 180) as ObsoleteQuantity,
    round(sumIf(i.QuantityOnHand * p.Cost, i.StockAging > 180), 2) as ObsoleteValueAtCost
FROM DimWarehouse AS w
LEFT JOIN FactInventory AS i ON w.WarehouseKey = i.WarehouseKey
LEFT JOIN DimProduct AS p ON i.ProductKey = p.ProductKey AND p.IsCurrent = 1
WHERE w.IsCurrent = 1
GROUP BY w.WarehouseKey, w.WarehouseID, w.WarehouseName, w.Location, w.WarehouseType;

-- View 9: Full Extended Purchases
CREATE OR REPLACE VIEW vw_purchases_extended_full AS
SELECT
    fp.PurchaseDateKey, d.DateKey, d.Year, d.Month, d.MonthName,
    fp.ProductKey, p.ProductName, p.SKU, p.Category, p.SubCategory, p.Brand, p.ListPrice, p.Cost,
    fp.VendorKey, v.VendorName, v.ContactPerson, v.City, v.Country, v.VendorRating, v.OnTimeDeliveryRate, v.QualityScore, v.PaymentTerms, v.VendorStatus,
    fp.PurchaseAmount, fp.QuantityBought, fp.UnitPrice, fp.DiscountAmount,
    (fp.PurchaseAmount - fp.DiscountAmount) as NetPurchaseAmount,
    round((fp.PurchaseAmount / greatest(fp.QuantityBought, 1)), 4) as EffectiveUnitCost,
    round(((fp.DiscountAmount / greatest(fp.PurchaseAmount, 1)) * 100), 2) as DiscountPercentage,
    round((((fp.PurchaseAmount - fp.DiscountAmount) - (fp.QuantityBought * p.Cost)) / greatest((fp.PurchaseAmount - fp.DiscountAmount), 1) * 100), 2) as CostSavingPercent
FROM FactPurchases AS fp
LEFT JOIN DimDate AS d ON fp.PurchaseDateKey = d.DateKey
LEFT JOIN DimProduct AS p ON fp.ProductKey = p.ProductKey AND p.IsCurrent = 1
LEFT JOIN DimVendor AS v ON fp.VendorKey = v.VendorKey AND v.IsCurrent = 1;

-- View 10: Vendor Performance
CREATE OR REPLACE VIEW vw_vendor_performance AS
SELECT
    v.VendorKey, v.VendorID, v.VendorName, v.ContactPerson, v.City, v.Country, v.VendorRating, v.OnTimeDeliveryRate, v.QualityScore, v.PaymentTerms, v.VendorStatus,
    uniq(fp.PurchaseDateKey) as PurchaseDays,
    uniq(fp.ProductKey) as UniqueProductsPurchased,
    sum(fp.QuantityBought) as TotalQuantityPurchased,
    sum(fp.PurchaseAmount) as TotalPurchaseAmount,
    sum(fp.PurchaseAmount - fp.DiscountAmount) as NetPurchaseAmount,
    sum(fp.DiscountAmount) as TotalDiscountsReceived,
    round((sum(fp.DiscountAmount) / greatest(sum(fp.PurchaseAmount), 1) * 100), 2) as AvgDiscountPercent,
    avg(fp.PurchaseAmount) as AvgOrderValue,
    sumIf(fp.QuantityBought, fp.UnitPrice > p.ListPrice * 0.8) as HighCostPurchasesQty,
    round(avg(v.VendorRating), 2) as EffectiveVendorRating,
    round(avg(v.OnTimeDeliveryRate), 2) as EffectiveOnTimeRate
FROM DimVendor AS v
LEFT JOIN FactPurchases AS fp ON v.VendorKey = fp.VendorKey
LEFT JOIN DimProduct AS p ON fp.ProductKey = p.ProductKey AND p.IsCurrent = 1
WHERE v.IsCurrent = 1
GROUP BY v.VendorKey, v.VendorID, v.VendorName, v.ContactPerson, v.City, v.Country, v.VendorRating, v.OnTimeDeliveryRate, v.QualityScore, v.PaymentTerms, v.VendorStatus;

-- View 11: Production Metrics
CREATE OR REPLACE VIEW vw_production_metrics AS
SELECT
    fp.ProductionDateKey, d.DateKey, d.Year, d.Month, d.MonthName,
    fp.ProductKey, p.ProductName, p.SKU, p.Category, p.Brand,
    fp.SupervisorKey, e.EmployeeName, e.JobTitle, e.Department,
    fp.UnitsProduced, fp.ProductionTimeHours, fp.ScrapRatePercent, fp.DefectCount,
    round((fp.UnitsProduced / greatest(fp.ProductionTimeHours, 1)), 2) as UnitsPerHour,
    round(((fp.UnitsProduced - (fp.UnitsProduced * fp.ScrapRatePercent / 100)) / greatest(fp.ProductionTimeHours, 1)), 2) as GoodUnitsPerHour,
    round((100 - fp.ScrapRatePercent), 2) as YieldPercent,
    round((fp.DefectCount / greatest(fp.UnitsProduced, 1) * 100), 2) as DefectRate,
    CASE
        WHEN fp.ScrapRatePercent > 10 THEN 'High'
        WHEN fp.ScrapRatePercent > 5 THEN 'Medium'
        ELSE 'Low'
    END as ScrapRiskLevel
FROM FactProduction AS fp
LEFT JOIN DimDate AS d ON fp.ProductionDateKey = d.DateKey
LEFT JOIN DimProduct AS p ON fp.ProductKey = p.ProductKey AND p.IsCurrent = 1
LEFT JOIN DimEmployee AS e ON fp.SupervisorKey = e.EmployeeKey AND e.IsCurrent = 1;

-- View 12: Feedback Analysis
CREATE OR REPLACE VIEW vw_feedback_analysis AS
SELECT
    fcf.FeedbackDateKey, d.DateKey, d.Year, d.Month, d.MonthName,
    fcf.CustomerKey, c.CustomerName, c.City, c.Country, c.CustomerSegment,
    fcat.CategoryName,
    fcf.FeedbackScore, fcf.ComplaintCount, fcf.ResolutionTimeHours, fcf.CSATScore,
    CASE
        WHEN fcf.FeedbackScore >= 4.5 THEN 'Excellent'
        WHEN fcf.FeedbackScore >= 3.5 THEN 'Good'
        WHEN fcf.FeedbackScore >= 2.5 THEN 'Fair'
        ELSE 'Poor'
    END as SatisfactionLevel,
    CASE
        WHEN fcf.ResolutionTimeHours <= 24 THEN 'Very Fast'
        WHEN fcf.ResolutionTimeHours <= 72 THEN 'Fast'
        WHEN fcf.ResolutionTimeHours <= 168 THEN 'Moderate'
        ELSE 'Slow'
    END as ResolutionSpeed
FROM FactCustomerFeedback fcf
LEFT JOIN DimDate AS d ON fcf.FeedbackDateKey = d.DateKey
LEFT JOIN DimCustomer AS c ON fcf.CustomerKey = c.CustomerKey AND c.IsCurrent = 1
LEFT JOIN DimFeedbackCategory AS fcat ON fcf.FeedbackCategoryKey = fcat.FeedbackCategoryKey;

-- View 13: Feedback Summary By Category
CREATE OR REPLACE VIEW vw_feedback_summary_by_category AS
SELECT
    fcat.FeedbackCategoryKey, fcat.CategoryName,
    count(*) as TotalFeedbackCount,
    avg(fcf.FeedbackScore) as AvgFeedbackScore,
    avg(fcf.CSATScore) as AvgCSATScore,
    sum(fcf.ComplaintCount) as TotalComplaints,
    avg(fcf.ResolutionTimeHours) as AvgResolutionTimeHours,
    countIf(fcf.FeedbackScore >= 4) as PositiveFeedbackCount,
    countIf(fcf.FeedbackScore < 3) as NegativeFeedbackCount
FROM FactCustomerFeedback AS fcf
LEFT JOIN DimFeedbackCategory AS fcat ON fcf.FeedbackCategoryKey = fcat.FeedbackCategoryKey
GROUP BY fcat.FeedbackCategoryKey, fcat.CategoryName;

-- View 14: Promotion Response
CREATE OR REPLACE VIEW vw_promotion_response AS
SELECT
    fpr.PromotionDateKey, d.DateKey, d.Year, d.Month, d.MonthName,
    fpr.ProductKey, p.ProductName, p.Category, p.SubCategory,
    fpr.StoreKey, s.StoreName, s.Region,
    fpr.PromotionKey, prom.PromotionName, prom.PromotionType, prom.DiscountPercentage,
    fpr.SalesDuringCampaign, fpr.DiscountUsageCount, fpr.CustomerUptakeRate, fpr.PromotionROI,
    CASE
        WHEN fpr.PromotionROI > 300 THEN 'Excellent'
        WHEN fpr.PromotionROI > 150 THEN 'Good'
        WHEN fpr.PromotionROI > 0 THEN 'Positive'
        ELSE 'Negative'
    END as ROICategory
FROM FactPromotionResponse AS fpr
LEFT JOIN DimDate AS d ON fpr.PromotionDateKey = d.DateKey
LEFT JOIN DimProduct AS p ON fpr.ProductKey = p.ProductKey AND p.IsCurrent = 1
LEFT JOIN DimStore AS s ON fpr.StoreKey = s.StoreKey AND s.IsCurrent = 1
LEFT JOIN DimPromotion AS prom ON fpr.PromotionKey = prom.PromotionKey;

-- View 15: Finance Asnalysis
CREATE OR REPLACE VIEW vw_finance_analysis AS
SELECT
    ff.InvoiceDateKey, d.DateKey, d.Year, d.Month, d.MonthName,
    ff.CustomerKey, c.CustomerName, c.City, c.Country, c.CustomerSegment, c.CreditLimit,
    ff.StoreKey, s.StoreName, s.Region,
    fcat.CategoryName,
    ff.InvoiceAmount, ff.PaymentDelayDays, ff.CreditUsagePct, ff.InterestCharges,
    ff.InvoiceAmount - ff.InterestCharges as NetInvoiceAmount,
    CASE
        WHEN ff.PaymentDelayDays = 0 THEN 'On Time'
        WHEN ff.PaymentDelayDays <= 10 THEN 'Slightly Late'
        WHEN ff.PaymentDelayDays <= 30 THEN 'Late'
        ELSE 'Very Late'
    END as PaymentStatus,
    CASE
        WHEN ff.CreditUsagePct >= 90 THEN 'Critical'
        WHEN ff.CreditUsagePct >= 75 THEN 'High'
        WHEN ff.CreditUsagePct >= 50 THEN 'Medium'
        ELSE 'Low'
    END as CreditRiskLevel
FROM FactFinance AS ff
LEFT JOIN DimDate AS d ON ff.InvoiceDateKey = d.DateKey
LEFT JOIN DimCustomer AS c ON ff.CustomerKey = c.CustomerKey AND c.IsCurrent = 1
LEFT JOIN DimStore AS s ON ff.StoreKey = s.StoreKey AND s.IsCurrent = 1
LEFT JOIN DimFinanceCategory fcat ON ff.FinanceCategoryKey = fcat.FinanceCategoryKey;

-- View 16: Return Analysis
CREATE OR REPLACE VIEW vw_returns_analysis AS
SELECT
    fr."ReturnDateKey",
    d."DateKey",
    d."Year",
    d."Month",
    d."MonthName",
    fr."CustomerKey",
    c."CustomerName",
    c."City",
    c."Country",
    c."CustomerSegment",
    fr."ProductKey",
    p."ProductName",
    p."Category",
    p."SubCategory",
    fr."StoreKey",
    s."StoreName",
    s."Region",
    rr."ReturnReasonName",
    fr."ReturnedQuantity",
    fr."RefundAmount",
    fr."RestockingFee",
    fr."RefundAmount" - fr."RestockingFee" as "NetRefundAmount",
    ROUND(((fr."RefundAmount" - fr."RestockingFee") / GREATEST(fr."RefundAmount", 1) * 100)::numeric, 2) as "NetRefundPercent"
FROM "FactReturns" AS fr
LEFT JOIN "DimDate" AS d ON fr."ReturnDateKey" = d."DateKey"
LEFT JOIN "DimCustomer" AS c ON fr."CustomerKey" = c."CustomerKey" AND c."IsCurrent" = 1
LEFT JOIN "DimProduct" AS p ON fr."ProductKey" = p."ProductKey" AND p."IsCurrent" = 1
LEFT JOIN "DimStore" AS s ON fr."StoreKey" = s."StoreKey" AND s."IsCurrent" = 1
LEFT JOIN "DimReturnReason" AS rr ON fr."ReturnReasonKey" = rr."ReturnReasonKey";

-- View 17: vw_return_analysis_by_product
CREATE OR REPLACE VIEW vw_return_analysis_by_product AS
SELECT
    p."ProductKey", p."ProductID", p."ProductName", p."Category", p."SubCategory", p."ListPrice", p."Cost",
    COUNT(*) as "TotalReturnEvents",
    SUM(fr."ReturnedQuantity") as "TotalReturnedQuantity",
    SUM(fr."RefundAmount") as "TotalRefundAmount",
    SUM(fr."RestockingFee") as "TotalRestockingFees",
    ROUND((SUM(fr."RefundAmount") / GREATEST(COUNT(DISTINCT fs."SalesDateKey"), 1))::numeric, 2) as "AvgRefundAmount",
    ROUND(((SUM(fr."ReturnedQuantity") / GREATEST((SELECT SUM("Quantity") FROM "FactSales" fs2 WHERE fs2."ProductKey" = p."ProductKey"), 1)) * 100)::numeric, 2) as "ReturnRatePercent",
    COUNT(CASE WHEN rr."ReturnReasonName" = 'Defective' THEN 1 END) as "DefectiveReturns",
    COUNT(CASE WHEN rr."ReturnReasonName" = 'Wrong Item' THEN 1 END) as "WrongItemReturns",
    COUNT(CASE WHEN rr."ReturnReasonName" = 'Changed Mind' THEN 1 END) as "ChangedMindReturns"
FROM "DimProduct" AS p
LEFT JOIN "FactReturns" AS fr ON p."ProductKey" = fr."ProductKey"
LEFT JOIN "FactSales" AS fs ON p."ProductKey" = fs."ProductKey"
LEFT JOIN "DimReturnReason" AS rr ON fr."ReturnReasonKey" = rr."ReturnReasonKey"
WHERE p."IsCurrent" = 1
GROUP BY p."ProductKey", p."ProductID", p."ProductName", p."Category", p."SubCategory", p."ListPrice", p."Cost";

-- View 17: Return Analysis (By Product)
CREATE OR REPLACE VIEW vw_return_analysis_by_product AS
SELECT
    p."ProductKey", p."ProductID", p."ProductName", p."Category", p."SubCategory", p."ListPrice", p."Cost",
    COUNT(*) as "TotalReturnEvents",
    SUM(fr."ReturnedQuantity") as "TotalReturnedQuantity",
    SUM(fr."RefundAmount") as "TotalRefundAmount",
    SUM(fr."RestockingFee") as "TotalRestockingFees",
    ROUND((SUM(fr."RefundAmount") / GREATEST(COUNT(DISTINCT fs."SalesDateKey"), 1))::numeric, 2) as "AvgRefundAmount",
    ROUND(((SUM(fr."ReturnedQuantity") / GREATEST((SELECT SUM("Quantity") FROM "FactSales" fs2 WHERE fs2."ProductKey" = p."ProductKey"), 1)) * 100)::numeric, 2) as "ReturnRatePercent",
    COUNT(CASE WHEN rr."ReturnReasonName" = 'Defective' THEN 1 END) as "DefectiveReturns",
    COUNT(CASE WHEN rr."ReturnReasonName" = 'Wrong Item' THEN 1 END) as "WrongItemReturns",
    COUNT(CASE WHEN rr."ReturnReasonName" = 'Changed Mind' THEN 1 END) as "ChangedMindReturns"
FROM "DimProduct" AS p
LEFT JOIN "FactReturns" AS fr ON p."ProductKey" = fr."ProductKey"
LEFT JOIN "FactSales" AS fs ON p."ProductKey" = fs."ProductKey"
LEFT JOIN "DimReturnReason" AS rr ON fr."ReturnReasonKey" = rr."ReturnReasonKey"
WHERE p."IsCurrent" = 1
GROUP BY p."ProductKey", p."ProductID", p."ProductName", p."Category", p."SubCategory", p."ListPrice", p."Cost";