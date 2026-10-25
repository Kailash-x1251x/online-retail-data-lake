from validate_data import (
    find_missing_fields,
    find_invalid_numeric_values,
    find_cancellation_invoices,
    find_duplicate_rows,
)



def create_investigation_datasets(df):
    investigation_datasets = {
        "missing_invoice_no" : find_missing_fields(df, "InvoiceNo"),
        "missing_stock_code" : find_missing_fields(df, "StockCode"),
        "missing_description" : find_missing_fields(df, "Description"),
        "missing_quantity" : find_invalid_numeric_values(df, "Quantity", issue="missing"),
        "missing_invoice_date" : find_missing_fields(df, "InvoiceDate"),
        "missing_unit_price" : find_invalid_numeric_values(df, "UnitPrice", issue="missing"),
        "missing_customer_id" : find_missing_fields(df, "CustomerID"),
        "missing_country" : find_missing_fields(df, "Country"),
        "non_positive_quantity" : find_invalid_numeric_values(df, "Quantity", issue="non_positive"),
        "non_positive_unit_price" : find_invalid_numeric_values(df, "UnitPrice", issue="non_positive"),
        "cancellation_invoices" : find_cancellation_invoices(df),
        "duplicate_rows" : find_duplicate_rows(df),
    }
    return investigation_datasets


if __name__ == "__main__":
    from read_data import create_spark_session, read_retail_data

    spark = create_spark_session()

    try:
        df = read_retail_data(spark)

        investigation_datasets = create_investigation_datasets(df)

        for name, dataset in investigation_datasets.items():
            print(f"{name}: {dataset.count()} rows")
    

    

    finally:
        spark.stop()