import os
import clickhouse_connect
from datetime import datetime, date, time
from decimal import Decimal
import psycopg2
from dotenv import load_dotenv
from collections import defaultdict

load_dotenv()

KEY_GENERATION = {
  "CustomerKey": "('x' || MD5(Sales.Customer.CustomerID::TEXT || COALESCE(Sales.Customer.ModifiedDate::TEXT, '')))::bit(32)::BIGINT",

  "ProductKey": "('x' || MD5(Production.Product.ProductID::TEXT || COALESCE(Production.Product.ModifiedDate::TEXT, '')))::bit(32)::BIGINT",

  "StoreKey": "('x' || MD5(Sales.Store.BusinessEntityID::TEXT || COALESCE(Sales.Store.ModifiedDate::TEXT, '')))::bit(32)::BIGINT",

  "EmployeeKey": "('x' || MD5(HumanResources.Employee.BusinessEntityID::TEXT || COALESCE(HumanResources.Employee.ModifiedDate::TEXT, '')))::bit(32)::BIGINT",

  "PromotionKey": "('x' || MD5(Sales.SpecialOffer.SpecialOfferID::TEXT || COALESCE(Sales.SpecialOffer.ModifiedDate::TEXT, '')))::bit(32)::BIGINT",

  "ProductCategoryKey": "('x' || MD5(Production.ProductCategory.ProductCategoryID::TEXT || Production.ProductCategory.Name::TEXT || Production.ProductCategory.Name::TEXT))::bit(32)::BIGINT",

  "WarehouseKey": "('x' || MD5(Production.Location.LocationID::TEXT || COALESCE(Production.Location.ModifiedDate::TEXT, '')))::bit(32)::BIGINT",

  "TerritoryKey": "('x' || MD5(Sales.SalesTerritory.TerritoryID::TEXT || Sales.SalesTerritory.Name::TEXT || Sales.SalesTerritory.\"group\"::TEXT || Sales.SalesTerritory.CountryRegionCode::TEXT))::bit(32)::BIGINT",

  "VendorKey": "('x' || MD5(Purchasing.Vendor.BusinessEntityID::TEXT || COALESCE(Purchasing.Vendor.ModifiedDate::TEXT, '')))::bit(32)::BIGINT",

  "RegionKey": "('x' || MD5(COALESCE(Sales.SalesTerritory.\"group\", '') || '|' || COALESCE(Person.CountryRegion.name, '')))::bit(32)::BIGINT"
}

def pg():
    return psycopg2.connect(
        dbname=os.getenv("ADVENTUREWORKS_DATABASE"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASS"),
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT")
    )

def ch():
    return clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST"),
        user=os.getenv("CLICKHOUSE_USER"),
        password=os.getenv("CLICKHOUSE_PASS"),
        port=8443,
        secure=True
    )

def check_insert(data, columns):
    for i, row in enumerate(data):
        if len(row) != len(columns):
            print(f"Row {i} length mismatch: expected {len(columns)}, got {len(row)}")
            print("Row content:", row)
            raise ValueError("Mismatched row found before insert.")

MAX_CLICKHOUSE_DATE = datetime(2100, 12, 31, 0, 0, 0, 0)


def clean_values_in_rows(rows, columns):
    date_keyword = 'date'
    indices = [i for i, col in enumerate(columns) if date_keyword in col.lower()]

    if rows:
        max_row_length = max(len(row) for row in rows)
        indices = [i for i in indices if i < max_row_length]

    cleaned_rows = []
    for row in rows:
        cleaned_row = []
        for value in row:
            if isinstance(value, Decimal):
                cleaned_row.append(float(value))
            elif value is None:
                cleaned_row.append(0)
            else:
                cleaned_row.append(value)

        ready_row = list(cleaned_row)

        for idx in indices:
            if idx >= len(ready_row):
                continue

            val = ready_row[idx]

            # 1) strings -> datetime
            if isinstance(val, str):
                try:
                    val = datetime.strptime(val, "%Y-%m-%d")
                except ValueError:
                    pass
                else:
                    ready_row[idx] = val

            # 2) date -> datetime (midnight)
            if isinstance(val, date) and not isinstance(val, datetime):
                val = datetime.combine(val, time.min)
                ready_row[idx] = val

            # re-read after any conversion
            val = ready_row[idx]

            # 3) clamp datetime only - handle timezone-aware datetimes
            if isinstance(val, datetime):
                # Convert to naive datetime if timezone-aware
                if val.tzinfo is not None:
                    val = val.replace(tzinfo=None)
                    ready_row[idx] = val

                # Now compare and clamp
                if val > MAX_CLICKHOUSE_DATE:
                    ready_row[idx] = MAX_CLICKHOUSE_DATE

        cleaned_rows.append(tuple(ready_row))

    return cleaned_rows

def partition_rows_fixed_batch(data, batch_size):
    partitioned = defaultdict(list)
    batch_id = 0

    for i, row in enumerate(data):
        if i % batch_size == 0:
            batch_id += 1
        partitioned[batch_id].append(row)

    return partitioned