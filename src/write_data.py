from pathlib import Path

from transform_data import (
    add_revenue_column,
    create_country_summary,
    create_product_summary,
    create_daily_sales_summary,
)


def write_parquet(df, output_path):
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    df.write.mode("overwrite").parquet(str(output_path))


def write_csv(df, output_path):
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    df.write.mode("Overwrite").option("header", True).csv(str(output_path))


if __name__ == "__main__":
    from read_data import create_spark_session, read_retail_data
    from validate_data import create_cleaned_data

    spark = create_spark_session()

    try:
        df = read_retail_data(spark)
        cleaned_df = create_cleaned_data(df)

        data_with_revenue = add_revenue_column(cleaned_df)

        country_summary = create_country_summary(data_with_revenue)
        product_summary = create_product_summary(data_with_revenue)
        daily_sales_summary = create_daily_sales_summary(data_with_revenue)


        write_parquet(
            cleaned_df, "data/processed/cleaned_retail"
        )

        write_csv(
            cleaned_df.limit(1000), "data/processed/cleaned_retail_sample_csv"
        )

        #Parquet & CSV for country_summary
        write_parquet(country_summary, "data/curated/country_summary")
        write_csv(country_summary, "data/curated/country_summary_csv")

        #Parquet & CSV for product summary
        write_parquet(product_summary, "data/curated/product_summary")
        write_csv(product_summary, "data/curated/product_summary_csv")

        #Parquet & CSV for daily_sales_summary
        write_parquet(daily_sales_summary, "data/curated/daily_sales_summary")
        write_csv(daily_sales_summary, "data/curated/daily_sales_summary_csv")

        print("Cleaned data saved successfully")
        print("Transformed data saved in 'Curated' successfully")


    finally:
        spark.stop()