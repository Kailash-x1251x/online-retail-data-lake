
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DecimalType,
    TimestampType,
)
from pyspark.sql import functions as F


retail_schema = StructType([
    StructField("InvoiceNo", StringType(), True),
    StructField("StockCode", StringType(), True),
    StructField("Description", StringType(), True),
    StructField("Quantity", IntegerType(), True),
    StructField("InvoiceDate", TimestampType(), True),
    StructField("UnitPrice", DecimalType(10, 2), True),
    StructField("CustomerID", StringType(), True),
    StructField("Country", StringType(), True)
])

def create_spark_session():
    """Create a local Spark session for the retail data pipeline."""
    spark = (
        SparkSession.builder
        .appName("OnlineRetailDataLake")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")
    return spark


def read_retail_data(spark):
    """Read the raw retail CSV file into a Spark DataFrame."""
    file_path = "data/raw/Online_Retail.csv"

    df = (
        spark.read
        .option("header", True)
        .option("timestampFormat", "M/d/yy H:mm")
        .schema(retail_schema)
        .csv(file_path)
    )

    return df

def inspect_data_quality(df):
    """Inspect common data-quality issues in the retail dataset."""

    print("\n---Missing Values---")

    df.select([
        F.sum(
            F.when(F.col(column).isNull(), 1).otherwise(0)
        ).alias(column)
        for column in[
            "InvoiceNo",
            "StockCode",
            "Description",
            "Quantity",
            "InvoiceDate",
            "UnitPrice",
            "CustomerID",
            "Country",
        ]
        ]).show()

    print("\n---Duplicate Rows---")
    print("Total Rows:", df.count())
    print("Distinct Rows:", df.distinct().count())

    print("\n---Quality Checks---")
    print("Negative Quantities:", df.filter(F.col("Quantity") < 0).count())
    print("Zero Quantities:", df.filter(F.col("Quantity") == 0).count())

    print("\n---Unit Price Checks---")
    print("Negative Prices:", df.filter(F.col("UnitPrice") < 0).count())
    print("Zero Prices:", df.filter(F.col("UnitPrice") == 0).count())

    print("\n--Cancellation Invoices---")
    print(
        "Cancellation Rows:",
        df.filter(F.col("InvoiceNo").startswith("C")).count()
    )

def inspect_suspicious_records(df):
    """Inspect unusual transactions before defining cleaning rules."""
    
    print("\n---Cancellation Invoice Examples---")
    df.filter(F.col("InvoiceNo").startswith("C")).show(5, truncate = False)

    print("\n---Negative quantity without C prefix---")
    df.filter((F.col("Quantity") < 0) & (~F.col("InvoiceNo").startswith("C"))).show(5, truncate = False)

    print("\n---Zero price examples---")
    df.filter(F.col("UnitPrice") == 0).show(5, truncate = False)

    print("\n---Negative price examples---")
    df.filter(F.col("UnitPrice") < 0).show(5, truncate = False)

    print("\n---Exact Dulpicate row examples---")
    duplicate_groups = (
        df.groupBy(*df.columns).count().filter(F.col("count") > 1).orderBy(F.col("count").desc())
    )
    duplicate_groups.show(5, truncate = False)



if __name__ == "__main__":
    spark = create_spark_session()

    try:
        df = read_retail_data(spark)
        inspect_data_quality(df)
        inspect_suspicious_records(df)

        print("\n--- Dataset Schema ---")
        df.printSchema()

        print("\n--- First Five Records ---")
        df.show(5, truncate=False)

        print("\n--- Total Row Count ---")
        print(df.count())

    finally:
        spark.stop()
