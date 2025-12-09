from datetime import datetime
import os
from dotenv import load_dotenv
from airflow.sdk import dag, task
import clickhouse_connect
import psycopg2
from decimal import Decimal
from collections import defaultdict
import pickle

load_dotenv()


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

def parse_sales_date(rows):
    parsed_rows = []
    for row in rows:
        sales_date = row[0]
        if isinstance(sales_date, str):
            sales_date = datetime.strptime(sales_date, "%Y-%m-%d").date()
        parsed_rows.append((sales_date, *row[1:]))
    return parsed_rows

FACT_SALES_COLUMNS = [
    "SalesDateKey",
    "CustomerID",
    "ProductID",
    "StoreID",
    "EmployeeID",
    "SalesOrderID",
    "SalesOrderDetailID",
    "QuantitySold",
    "SalesRevenue",
    "DiscountAmount",
    "NumberOfTransactions",
    "UnitPrice",
    "UnitPriceDiscount",
    "LineTotal"
]

def get_molded_data_for_testing():
    from decimal import Decimal
    conn = pg()
    cur = conn.cursor()

    qry = """
          SELECT to_char(soh.OrderDate, 'YYYY-MM-DD')                                                 AS SalesDateKey, 
                 soh.CustomerID                                                                     AS CustomerID,
                 sod.ProductID                                                                      AS ProductID,
                 COALESCE(c.StoreID, 0)                                                             AS StoreID,
                 COALESCE(soh.SalesPersonID, 0)                                                     AS EmployeeID,
                 sod.SalesOrderID                                                                   AS SalesOrderID,
                 sod.SalesOrderDetailID                                                             AS SalesOrderDetailID,
                 sod.OrderQty                                                                       AS QuantitySold,
                 CAST(sod.UnitPrice * sod.OrderQty * (1 - sod.UnitPriceDiscount) AS DECIMAL(18, 2)) AS SalesRevenue,
                 CAST(sod.UnitPrice * sod.OrderQty * sod.UnitPriceDiscount AS DECIMAL(18, 2))       AS DiscountAmount,
                 1                                                                                  AS NumberOfTransactions,
                 CAST(sod.UnitPrice AS DECIMAL(18, 2))                                              AS UnitPrice,
                 CAST(sod.UnitPriceDiscount AS DECIMAL(18, 4))                                      AS UnitPriceDiscount,
                 CAST(soh.TotalDue AS DECIMAL(18, 2))                                              AS LineTotal
          FROM Sales.SalesOrderDetail sod
                   JOIN Sales.SalesOrderHeader soh
                        ON sod.SalesOrderID = soh.SalesOrderID
                   JOIN Sales.Customer c
                        ON soh.CustomerID = c.CustomerID
          ORDER BY sod.SalesOrderID, sod.SalesOrderDetailID;
          """

    cur.execute(qry)
    rows = cur.fetchall()

    cur.close()
    conn.close()

    cleaned_rows = []
    for row in rows:
        cleaned_row = []
        for val in row:
            if isinstance(val, Decimal):
                cleaned_row.append(float(val))
            elif val is None:
                cleaned_row.append(0)  # Handle NULLs
            else:
                cleaned_row.append(val)
        cleaned_rows.append(tuple(cleaned_row))

    return parse_sales_date(cleaned_rows)


@dag(
    dag_id="adventureworks_fact_population",
    dag_display_name="Populate Fact Tables",
    schedule="@hourly",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["adventureworks", "facts", "population"]
)
def populate():
    @task
    def mold_data():
        conn = pg()
        cur = conn.cursor()

        qry = """
              SELECT to_char(soh.OrderDate, 'YYYY-MM-DD')                                                 AS SalesDateKey, \
                     soh.CustomerID                                                                     AS CustomerID, \
                     sod.ProductID                                                                      AS ProductID, \
                     COALESCE(c.StoreID, 0)                                                             AS StoreID, \
                     COALESCE(soh.SalesPersonID, 0)                                                     AS EmployeeID, \
                     sod.SalesOrderID                                                                   AS SalesOrderID, \
                     sod.SalesOrderDetailID                                                             AS SalesOrderDetailID, \
                     sod.OrderQty                                                                       AS QuantitySold, \
                     CAST(sod.UnitPrice * sod.OrderQty * (1 - sod.UnitPriceDiscount) AS DECIMAL(18, 2)) AS SalesRevenue, \
                     CAST(sod.UnitPrice * sod.OrderQty * sod.UnitPriceDiscount AS DECIMAL(18, 2))       AS DiscountAmount, \
                     1                                                                                  AS NumberOfTransactions, \
                     CAST(sod.UnitPrice AS DECIMAL(18, 2))                                              AS UnitPrice, \
                     CAST(sod.UnitPriceDiscount AS DECIMAL(18, 4))                                      AS UnitPriceDiscount, \
                     CAST(soh.TotalDue AS DECIMAL(18, 2))                                              AS LineTotal
              FROM Sales.SalesOrderDetail sod
                       JOIN Sales.SalesOrderHeader soh
                            ON sod.SalesOrderID = soh.SalesOrderID
                       JOIN Sales.Customer c
                            ON soh.CustomerID = c.CustomerID
              ORDER BY sod.SalesOrderID, sod.SalesOrderDetailID; \
              """

        cur.execute(qry)
        rows = cur.fetchall()

        cur.close()
        conn.close()

        cleaned_rows = []
        for row in rows:
            cleaned_row = []
            for val in row:
                if isinstance(val, Decimal):
                    cleaned_row.append(float(val))
                elif val is None:
                    cleaned_row.append(0)  # Handle NULLs
                else:
                    cleaned_row.append(val)
            cleaned_rows.append(tuple(cleaned_row))

        molded = parse_sales_date(cleaned_rows)

        with open('/tmp/fact_sales_data.pkl', 'wb') as f:
            pickle.dump(molded, f)

        return '/tmp/fact_sales_data.pkl'

    @task
    def load_data(data_path):

        with open(data_path, 'rb') as f:
            data = pickle.load(f)

        try:
            client = ch()

            partitioned = defaultdict(list)
            for row in data:
                partitioned[row[0]].append(row)

            total_inserted = 0

            for partition_key, rows in partitioned.items():
                print(f"Inserting {len(rows)} rows for partition {partition_key}")
                client.insert(
                    "ADVENTUREWORKS_DWS.FactSales",
                    rows,
                    column_names=FACT_SALES_COLUMNS
                )
                total_inserted += len(rows)

            print(f"✅ Inserted {total_inserted} rows across {len(partitioned)} partitions")
            return total_inserted

        except Exception as e:
            print(f"Error loading data: {str(e)}")
            print(f"First row causing issue: {data[0] if data else 'No data'}")
            raise

    md = mold_data.override(task_id="mold_existing_data")()
    ld = load_data.override(task_id="load_to_clickhouse")(md)

    md >> ld


populate()