-- Change all ValidTo, EndDate Dates to Nullable(Date)
-- Create Database
CREATE DATABASE IF NOT EXISTS ADVENTUREWORKS_DWS;

-- Create Date Dimension
DROP TABLE IF EXISTS ADVENTUREWORKS_DWS.DimDate;
CREATE TABLE IF NOT EXISTS ADVENTUREWORKS_DWS.DimDate
(
    -- Surogātatslēga
    DateKey            Date,

    -- Datumu vienības
    Year               UInt16,
    Quarter            UInt8,
    Month              UInt8,
    MonthName          LowCardinality(String),
    Week               UInt8,
    DayOfWeek          UInt8,
    DayName            LowCardinality(String),
    DayOfMonth         UInt8,
    DayOfYear          UInt16,
    WeekOfYear         UInt8,

    -- Boolean vērtības
    IsWeekend          UInt8,
    IsHoliday          UInt8,

    -- Apzīmētāji
    HolidayName        LowCardinality(String),
    FiscalYear         UInt16,
    FiscalQuarter      UInt8,
    FiscalMonth        UInt8,
    Season             LowCardinality(String)
)
ENGINE = MergeTree
PRIMARY KEY DateKey
ORDER BY DateKey;

-- Create Customer Dimension
DROP TABLE IF EXISTS ADVENTUREWORKS_DWS.DimCustomer;
CREATE TABLE IF NOT EXISTS ADVENTUREWORKS_DWS.DimCustomer (
    -- Surogātatslēga & naturālā atslēga
    CustomerKey UInt64,
    CustomerID UInt32,

    -- Pircēja Atribūti
    CustomerName String,
    Email String,
    Phone String,
    City LowCardinality(String),
    StateProvince LowCardinality(String),
    Country LowCardinality(String),
    PostalCode String,

    -- Apzīmētāji
    CustomerSegment LowCardinality(String),
    CustomerType LowCardinality(String),
    AccountStatus LowCardinality(String),
    CreditLimit Decimal(18, 2),
    AnnualIncome Decimal(18, 2),
    YearsSinceFirstPurchase Int32,

    -- Vēsturei aktuālie mainīgie
    ValidFromDate Date,
    ValidToDate Nullable(Date),
    IsCurrent UInt8,
    SourceUpdateDate Date,
    EffectiveStartDate Date,
    EffectiveEndDate Nullable(Date),

    -- Versionēšana
    Version UInt64
) ENGINE = ReplacingMergeTree(Version) PARTITION BY toYYYYMM(ValidFromDate)
ORDER BY
    (CustomerID, ValidFromDate);

-- Create Product Dimension
DROP TABLE IF EXISTS ADVENTUREWORKS_DWS.DimProduct;
CREATE TABLE IF NOT EXISTS ADVENTUREWORKS_DWS.DimProduct (
    -- Atslēgas
    ProductKey UInt64,
    ProductID UInt32,

    -- Atribūti
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

    -- Vēstures Dati
    ValidFromDate Date,
    ValidToDate Nullable(Date),
    IsCurrent UInt8,
    SourceUpdateDate Date,
    EffectiveStartDate Date,
    EffectiveEndDate Nullable(Date),

    -- Versionēšana
    Version UInt64
) ENGINE = ReplacingMergeTree(Version) PARTITION BY toYYYYMM(ValidFromDate)
ORDER BY
    (ProductID, ValidFromDate);

-- Create Store Dimension
DROP TABLE IF EXISTS ADVENTUREWORKS_DWS.DimStore;
CREATE TABLE IF NOT EXISTS ADVENTUREWORKS_DWS.DimStore (
    -- Atslēgas
    StoreKey UInt64,
    StoreID UInt32,

    -- Atribūti
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

    -- Vēstures Atribūti
    ValidFromDate Date,
    ValidToDate Nullable(Date),
    IsCurrent UInt8,
    SourceUpdateDate Date,

    -- Versionēšana
    Version UInt64
) ENGINE = ReplacingMergeTree(Version) PARTITION BY toYYYYMM(ValidFromDate)
ORDER BY
    (StoreID, ValidFromDate);

-- Create Employee Dimension
DROP TABLE IF EXISTS ADVENTUREWORKS_DWS.DimEmployee;
CREATE TABLE IF NOT EXISTS ADVENTUREWORKS_DWS.DimEmployee
(
    -- Atslēgas
    EmployeeKey          UInt64,
    EmployeeID           Int32,

    -- Atribūti
    EmployeeName         String,
    JobTitle             LowCardinality(String),
    Department           LowCardinality(String),

    -- Pašreferencētā atslēga
    ReportingManagerKey  UInt64,

    -- Aprakstošie lielumi
    HireDate             Date,
    EmployeeStatus       LowCardinality(String),
    Region               LowCardinality(String),
    Territory            LowCardinality(String),
    SalesQuota           Decimal(18,2),

    -- Vēstures atribūti
    ValidFromDate        Date,
    ValidToDate          Nullable(Date),
    IsCurrent            UInt8,
    SourceUpdateDate     Date,

    -- Versionēšana
    Version              UInt64
)
ENGINE = ReplacingMergeTree(Version)
PARTITION BY toYYYYMM(ValidFromDate)
ORDER BY (EmployeeID, ValidFromDate);

-- Create Vendor Dimension
DROP TABLE IF EXISTS ADVENTUREWORKS_DWS.DimVendor;
CREATE TABLE IF NOT EXISTS ADVENTUREWORKS_DWS.DimVendor (

    -- Atslēgas
    VendorKey UInt64,
    VendorID UInt32,

    -- Atribūti
    VendorName String,
    ContactPerson String,
    Email String,
    Phone String,
    Address String,
    City LowCardinality(String),
    Country LowCardinality(String),
    VendorRating DECIMAL(3,2),
    OnTimeDeliveryRate DECIMAL(5,2),
    QualityScore DECIMAL(5,2),
    PaymentTerms String,
    VendorStatus LowCardinality(String),

    -- Versionēšana
    ValidFromDate Date,
    ValidToDate Nullable(Date),
    IsCurrent UInt8,
    SourceUpdateDate Date
) ENGINE = ReplacingMergeTree(IsCurrent) PARTITION BY toYYYYMM(ValidFromDate)
ORDER BY
    (VendorID, ValidFromDate);

-- Create Promotion Dimension
DROP TABLE IF EXISTS ADVENTUREWORKS_DWS.DimPromotion;
CREATE TABLE IF NOT EXISTS ADVENTUREWORKS_DWS.DimPromotion (

    -- Atslēgas
    PromotionKey UInt64,
    PromotionID UInt32,

    -- Atribūti
    PromotionName String,
    PromotionDescription String,
    PromotionType LowCardinality(String),
    DiscountPercentage DECIMAL(5, 2),
    DiscountAmount DECIMAL(18, 2),
    PromotionStatus LowCardinality(String),
    TargetCustomerSegment Nullable(String),

    -- Paplašinājums
    CampaignID UInt32,
    TargetProductKey Nullable(UInt64),

    -- Versionēšana
    StartDate Date,
    EndDate Nullable(Date),
    IsActive UInt8
) ENGINE = ReplacingMergeTree(IsActive) PRIMARY KEY (PromotionKey, PromotionID)
ORDER BY
    (PromotionKey, PromotionID);

-- Create Feedback Category Dimension
DROP TABLE IF EXISTS ADVENTUREWORKS_DWS.DimFeedbackCategory;
CREATE TABLE IF NOT EXISTS ADVENTUREWORKS_DWS.DimFeedbackCategory (
    -- Atslēgas
    FeedbackCategoryKey UInt64,
    FeedbackCategoryID UInt32,

    -- Atribūti
    CategoryName String,
    CategoryDescription String
) ENGINE = MergeTree() PRIMARY KEY (FeedbackCategoryKey, FeedbackCategoryID)
ORDER BY
    (FeedbackCategoryKey, FeedbackCategoryID);

-- Create Return Reason Dimension
DROP TABLE IF EXISTS ADVENTUREWORKS_DWS.DimReturnReason;
CREATE TABLE IF NOT EXISTS ADVENTUREWORKS_DWS.DimReturnReason (

    -- Atslēgas
    ReturnReasonKey UInt64,
    ReturnReasonID UInt32,

    -- Atribūti
    ReturnReasonName LowCardinality(String),
    ReturnReasonDescription String
) ENGINE = MergeTree() PRIMARY KEY ReturnReasonKey
ORDER BY
    (ReturnReasonKey, ReturnReasonID);

-- Create Warehouse Dimension
DROP TABLE IF EXISTS ADVENTUREWORKS_DWS.DimWarehouse;
CREATE TABLE IF NOT EXISTS ADVENTUREWORKS_DWS.DimWarehouse (

    -- Atslēgas
    WarehouseKey UInt64,
    WarehouseID UInt32,

    -- Atribūti
    WarehouseName String,
    Location String,
    WarehouseType LowCardinality(String),
    ManagerKey UInt64,

    -- Versionēšana
    ValidFromDate Date,
    ValidToDate Nullable(Date),
    IsCurrent UInt8
) ENGINE = ReplacingMergeTree(IsCurrent) PRIMARY KEY (WarehouseKey, WarehouseID)
ORDER BY
    (WarehouseKey, WarehouseID);

-- Create Sales Territory Dimension
DROP TABLE IF EXISTS ADVENTUREWORKS_DWS.DimSalesTerritory;
CREATE TABLE IF NOT EXISTS ADVENTUREWORKS_DWS.DimSalesTerritory (

    -- Atslēgas
    TerritoryKey UInt64,
    TerritoryID UInt32,

    -- Atribūti
    TerritoryName String,
    SalesRegion LowCardinality(String),
    Country LowCardinality(String),
    Manager String,
    SalesTarget DECIMAL(18, 2)
) ENGINE = MergeTree() PRIMARY KEY (TerritoryKey, TerritoryID)
ORDER BY
    (TerritoryKey, TerritoryID);

-- Create Customer Segment Dimension
DROP TABLE IF EXISTS ADVENTUREWORKS_DWS.DimCustomerSegment;
CREATE TABLE IF NOT EXISTS ADVENTUREWORKS_DWS.DimCustomerSegment (

    -- Atslēgas
    SegmentKey UInt64,
    SegmentID UInt32,

    -- Atribūti
    SegmentName LowCardinality(String),
    SegmentDescription String,
    DiscountTierStart Decimal(5, 2),
    DiscountTierEnd Decimal(5, 2)
) ENGINE = MergeTree() PRIMARY KEY (SegmentKey, SegmentID)
ORDER BY
    (SegmentKey, SegmentID);

-- Create Aging Tier Dimension
DROP TABLE IF EXISTS ADVENTUREWORKS_DWS.DimAgingTier;
CREATE TABLE IF NOT EXISTS ADVENTUREWORKS_DWS.DimAgingTier (

    -- Atslēgas
    AgingTierKey UInt64,
    AgingTierID UInt32,

    -- Atribūti
    AgingTierName LowCardinality(String),
    MinAgingDays UInt32,
    MaxAgingDays UInt32
) ENGINE = MergeTree() PRIMARY KEY (AgingTierKey)
ORDER BY
    (AgingTierKey);

-- Create Finance Category Dimension
DROP TABLE IF EXISTS ADVENTUREWORKS_DWS.DimFinanceCategory;
CREATE TABLE IF NOT EXISTS ADVENTUREWORKS_DWS.DimFinanceCategory (
    -- Atslēgas
    FinanceCategoryKey UInt64,
    FinanceCategoryID UInt32,

    -- Atribūti
    CategoryName LowCardinality(String),
    CategoryDescription String
) ENGINE = MergeTree() PRIMARY KEY (FinanceCategoryKey, FinanceCategoryID)
ORDER BY
    (FinanceCategoryKey, FinanceCategoryID);

-- Create Region Dimension
DROP TABLE IF EXISTS ADVENTUREWORKS_DWS.DimRegion;
CREATE TABLE IF NOT EXISTS ADVENTUREWORKS_DWS.DimRegion (
    -- Atslēgas
    RegionKey UInt64,
    RegionID UInt32,

    -- Atribūti
    RegionName String,
    Country LowCardinality(String),
    Continent LowCardinality(String),
    TimeZone LowCardinality(String)
) ENGINE = MergeTree() PRIMARY KEY (RegionKey, RegionID)
ORDER BY
    (RegionKey, RegionID);

-- Create Product Category Dimension
DROP TABLE IF EXISTS ADVENTUREWORKS_DWS.DimProductCategory;
CREATE TABLE IF NOT EXISTS ADVENTUREWORKS_DWS.DimProductCategory (
    -- Atslēgas
    ProductCategoryKey UInt64,
    ProductCategoryID UInt32,

    -- Atribūti
    CategoryName String,
    CategoryDescription String
) ENGINE = MergeTree PRIMARY KEY (ProductCategoryKey, ProductCategoryID)
ORDER BY
    (ProductCategoryKey, ProductCategoryID);