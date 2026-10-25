from pyspark.sql import functions as F
from pyspark.sql import Window

def find_missing_required_fields(df):
    """Find rows missing required fields or containing blank required text."""

    required_columns = [
        "InvoiceNo",
        "StockCode",
        "Description",
        "Quantity",
        "InvoiceDate",
        "UnitPrice",
        "CustomerID",
        "Country",
    ]

    missing_condition = None

    for column in required_columns:
        condition = F.col(column).isNull()

        #Treat blank text as missing too
        if column in ["InvoiceNo", "StockCode", "Description", "CustomerID", "Country"]:
            condition = condition | (F.trim(F.col(column)) == "")

        if missing_condition is None:
            missing_condition = condition
        else:
            missing_condition = missing_condition | condition

    return df.filter(missing_condition)


def find_missing_fields(df, column_name):
    """Find rows where a specified field is missing or blank"""

    condition = F.col(column_name).isNull()

    #Text columns can also contain empty or whitespace only values
    text_columns = [
        "InvoiceNo",
        "StockCode",
        "Description",
        "CustomerID",
        "Country",
    ]

    if column_name in text_columns:
        condition = condition | (F.trim(F.col(column_name)) == "")

    return df.filter(condition)


def find_invalid_numeric_values(df, column_name, issue="all"):
    """Finds missing or non-positive values in a numeric column"""

    if issue == "missing":
        condition = F.col(column_name).isNull()

    elif issue == "non_positive":
        condition = F.col(column_name) <= 0

    else:
        condition = (
                F.col(column_name).isNull() | (F.col(column_name) <= 0)
            )

    return df.filter(condition)


def create_cleaned_data(df):
    """Keep complete, positive-quantity sales records without cancellations"""

    cleaned_df = (
        df.filter(
            F.col("InvoiceNo").isNotNull()
            & (~F.col("InvoiceNo").startswith("C"))
            & F.col("StockCode").isNotNull()
            & (F.trim(F.col("StockCode")) != "")
            & F.col("Description").isNotNull()
            & (F.trim(F.col("Description")) != "")
            & (F.col("Quantity") > 0)
            & F.col("InvoiceDate").isNotNull()
            & (F.col("UnitPrice") > 0)
            & F.col("CustomerID").isNotNull()
            & (F.trim(F.col("CustomerID")) != "")
            & F.col("Country").isNotNull()
            & (F.trim(F.col("Country")) != "")
        )
        .dropDuplicates()
    )

    return cleaned_df


def find_cancellation_invoices(df):
    """Find invoices marked as cancellations ( Start with C )"""

    return df.filter(F.col("InvoiceNo").startswith("C"))


def find_duplicate_rows(df):
    """Return every occurance of an exact duplicate row"""

    window_spec = Window.partitionBy(*df.columns)

    df_with_count = df.withColumn("duplicate_count", F.count("*").over(window_spec))

    return (df_with_count.filter(F.col("duplicate_count") > 1).drop("duplicate_count"))



if __name__ == "__main__":
    from read_data import create_spark_session, read_retail_data

    spark = create_spark_session()

    try:
        df = read_retail_data(spark)

        missing_df = find_missing_required_fields(df)
        cleaned_df = create_cleaned_data(df)

        #Missing or blank values in text columns
        missing_invoice_no_df = find_missing_fields(df, "InvoiceNo")
        missing_stock_code_df = find_missing_fields(df, "StockCode")
        missing_description_df = find_missing_fields(df, "Description")
        missing_customer_id_df = find_missing_fields(df, "CustomerID")
        missing_country_df = find_missing_fields(df, "Country")

        #Missing values in numeric columns
        missing_quantity_df = find_invalid_numeric_values(df, "Quantity", issue="missing")
        missing_unit_price_df = find_invalid_numeric_values(df, "UnitPrice", issue="missing")

        #Non-positive in numeric columns
        non_positive_quantity_df = find_invalid_numeric_values(df, "Quantity", issue="non_positive")
        non_positive_unit_price_df = find_invalid_numeric_values(df, "UnitPrice", issue="non_positive")

        #Missing Invoice Dates
        missing_invoice_date_df = find_missing_fields(df, "InvoiceDate")


        cancellation_df = find_cancellation_invoices(df)
        duplicate_rows_df = find_duplicate_rows(df)


        print("Original rows:", df.count())
        print("Rows with missing required fields:", missing_df.count())
        print("Cleaned rows:", cleaned_df.count())

        print("Rows missing InvoiceNo:", missing_invoice_no_df.count())
        print("Rows missing StockCode:", missing_stock_code_df.count())
        print("Rows missing Description:", missing_description_df.count())
        print("Rows missing InvoiceDate:", missing_invoice_date_df.count())
        print("Rows missing Quantity:", missing_quantity_df.count())
        print("Rows missing UnitPrice:", missing_unit_price_df.count())
        print("Rows missing CustomerID:", missing_customer_id_df.count())
        print("Rows missing Country:", missing_country_df.count())

        print("Non-positive Quantity:", non_positive_quantity_df.count())
        print("Non-posititve UnitPrice:", non_positive_unit_price_df.count())


        print("Cancellation invoice rows:", cancellation_df.count())
        print("Rows belonging to duplicate groups:", duplicate_rows_df.count())


        print("\n--Cleaned Data Sample---")
        cleaned_df.show(10, truncate=False)

        print("\n---Missing Customer ID Sample---")
        missing_customer_id_df.show(5, truncate=False)

        print("\n---Missing Description Sample---")
        missing_description_df.show(5, truncate=False)


        


    finally:
        spark.stop()

