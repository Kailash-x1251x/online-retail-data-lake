from pyspark.sql import functions as F


#Create and add new column "CalculatedRevenue" by using the formula "Revenue = Quantity * Unit Price"
def add_revenue_column(df):
    return df.withColumn("CalculatedRevenue", F.round(F.col("Quantity") * F.col("UnitPrice"), 2))


def create_country_summary(df):
    return (
        df.groupBy("Country").agg(
            F.sum("CalculatedRevenue").alias("TotalRevenue"),
            F.count("*").alias("TransactionLineCount")
        )
    )


def create_product_summary(df):
    #Total quantity sold, revenue and number of transaction lines per product
    
    return (
        df.groupBy("StockCode").agg(
            F.sum("Quantity").alias("TotalQuantitySold"),
            F.sum("CalculatedRevenue").alias("TotalRevenue"),
            F.count("*").alias("TransactionLineCount")
        )
    )


def create_daily_sales_summary(df):
    #Total revenue and transaction-line count per date
    
    df_with_SalesDate = df.withColumn("SalesDate", F.to_date("InvoiceDate"))

    return (
        df_with_SalesDate.groupBy("SalesDate").agg(
                F.sum("CalculatedRevenue").alias("TotalRevenue"),
                F.count("*").alias("TransactionLineCount")
        )
    )
    



if __name__ == "__main__":
    from read_data import create_spark_session, read_retail_data
    from validate_data import create_cleaned_data

    spark = create_spark_session()

    try:
        df = read_retail_data(spark)
        cleaned_df = create_cleaned_data(df)
        data_with_revenue_column = add_revenue_column(cleaned_df)

        country_summary = create_country_summary(data_with_revenue_column)
        product_summary = create_product_summary(data_with_revenue_column)
        daily_sales_summary = create_daily_sales_summary(data_with_revenue_column)


        #df with added column
        print("\n---DF with Revenue Sample---")
        data_with_revenue_column.show(5, truncate=False)

        #Transformation on country summary with Total Revenue and TransactionCount
        print("\n---Country Summary Sample---")
        country_summary.show(5, truncate=False)

        #Transformation on product summary with Total quantity sold, revenue and number of lines of transaction per product
        print("\n---Product Summary Sample---")
        product_summary.show(5, truncate=False)

        #Transformation on Daily Sales with total revenue and transaction lines per date
        print("\n---Daily Sales Summary---")
        daily_sales_summary.show(5, truncate=False)

    finally:
        spark.stop()
