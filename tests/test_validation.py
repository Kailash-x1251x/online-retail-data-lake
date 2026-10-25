import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from pyspark.sql import SparkSession
from validate_data import create_cleaned_data

def test_cleaned_data_has_positive_quantity_and_price():
    spark = (SparkSession.builder.master("local[1]").appName("ValidationTest").getOrCreate())

    try:
        data = [
            ("1001", "A1", "Product A", 2, "2026-01-01 10:00:00", 5.0, "123", "UK"),
            ("1002", "A2", "Product B", 0, "2026-01-01 10:00:00", 3.0, "124", "UK"),
            ("1003", "A3", "Product C", 1, "2026-01-01 10:00:00", 0.0, "125", "UK"),
        ]

        columns = [
            "InvoiceNo", "StockCode", "Description", "Quantity", "InvoiceDate", "UnitPrice", "CustomerID", "Country"
        ]

        df = spark.createDataFrame(data, columns)
        cleaned_df = create_cleaned_data(df)

        assert cleaned_df.filter("Quantity <= 0").count() == 0
        assert cleaned_df.filter("UnitPrice <= 0").count() == 0
        assert cleaned_df.count() == 1

    finally:

        spark.stop()


def test_cleaned_data_excludes_missing_customer_id():
    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("MissingCustomerTest")
        .getOrCreate()
    )

    try:
        data = [
            ("1001", "A1", "Product A", 2, "2026-01-01 10:00:00", 5.0, "123", "UK"),
            ("1002", "A2", "Product B", 1, "2026-01-01 10:00:00", 3.0, None, "UK"),
        ]

        columns = [
            "InvoiceNo", "StockCode", "Description", "Quantity",
            "InvoiceDate", "UnitPrice", "CustomerID", "Country"
        ]

        df = spark.createDataFrame(data, columns)
        cleaned_df = create_cleaned_data(df)

        assert cleaned_df.count() == 1
        assert cleaned_df.filter("CustomerID IS NULL").count() == 0

    finally:
        spark.stop()