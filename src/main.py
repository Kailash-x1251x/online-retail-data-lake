from read_data import create_spark_session, read_retail_data
from validate_data import create_cleaned_data
from transform_data import (
    add_revenue_column,
    create_country_summary,
    create_product_summary,
    create_daily_sales_summary,
)

from investigate import create_investigation_datasets

from write_data import write_parquet, write_csv


def run_pipeline():
    spark = create_spark_session()

    try:
        # Read raw data
        df = read_retail_data(spark)

        # Clean data
        cleaned_df = create_cleaned_data(df)

        # Calculate revenue
        data_with_revenue = add_revenue_column(cleaned_df)

        # Create summaries
        country_summary = create_country_summary(data_with_revenue)
        product_summary = create_product_summary(data_with_revenue)
        daily_sales_summary = create_daily_sales_summary(data_with_revenue)

        # Save processed data
        write_parquet(cleaned_df, "data/processed/cleaned_retail")
        write_csv(cleaned_df.limit(1000), "data/processed/cleaned_retail_sample_csv")

        # Save curated summaries (transformed data)
        write_parquet(country_summary, "data/curated/country_summary")
        write_csv(country_summary, "data/curated/country_summary_csv")

        write_parquet(product_summary, "data/curated/product_summary")
        write_csv(product_summary, "data/curated/product_summary_csv")

        write_parquet(daily_sales_summary, "data/curated/daily_sales_summary")
        write_csv(daily_sales_summary, "data/curated/daily_sales_summary_csv")


        #Save rejected investigation datasets
        investigation_datasets = create_investigation_datasets(df)

        for name, dataset in investigation_datasets.items():
            write_parquet(dataset, f"data/rejected/{name}")
            write_csv(dataset, f"data/rejected/{name}_csv")
            

        print("Pipeline completed successfully")

    finally:

        spark.stop()

if __name__ == "__main__":
    run_pipeline()