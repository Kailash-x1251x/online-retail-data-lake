# Online Retail Data Lake

A batch data engineering project built with Python, PySpark, and Amazon S3 to process online retail transactions, enforce data quality rules, generate sales summaries, and organize datasets for downstream analysis.

## Project Overview

This project implements a batch data processing pipeline using PySpark. It ingests raw retail transaction data, validates and cleans records, identifies data quality issues, calculates transaction-line revenue, and generates analytical summaries.

The project separates cleaned transaction data, curated analytical outputs, and investigation datasets. Processed outputs are stored locally in Parquet and CSV formats and uploaded to Amazon S3 separately using the AWS CLI.

## Architecture

```text
                   Online Retail CSV
                           |
                           v
                    PySpark Ingestion
                           |
                           v
                  Data Quality Validation
                           |
                +----------+-----------+
                |                      |
                v                      v
         Cleaned Transactions    Investigation Datasets
                |                      |
                v                      v
         Revenue Calculation     Quality Issue Reports
                |
                v
          Sales Aggregations
                |
         +------+------+ 
         |      |      |
         v      v      v
      Country Product Daily
      Summary Summary Summary
         |      |      |
         +------+------+
                |
                v
       Local Parquet / CSV Outputs
                |
                v
       AWS CLI Upload to Amazon S3
```

**Note:** PySpark processing runs locally. Uploading files to S3 is a separate step performed using the AWS CLI; the Python pipeline does not upload files automatically.

## Technology Stack

- **Python** — pipeline logic and orchestration
- **Apache Spark / PySpark** — data ingestion, validation, transformations, and aggregations
- **Parquet** — columnar storage for processed datasets
- **CSV** — source data and convenient output inspection
- **Amazon S3** — cloud object storage
- **AWS CLI** — uploading and verifying files in S3
- **pytest** — automated validation tests
- **Git and GitHub** — version control and project hosting

## Dataset

The project uses the Online Retail transaction dataset.

- **Source file:** `Online_Retail.csv`
- **Original row count:** 541,909
- **Original column count:** 8

The source dataset contains:

| Column | Description |
|---|---|
| `InvoiceNo` | Invoice identifier |
| `StockCode` | Product identifier |
| `Description` | Product description |
| `Quantity` | Quantity recorded on the transaction line |
| `InvoiceDate` | Invoice date and time |
| `UnitPrice` | Unit price |
| `CustomerID` | Customer identifier |
| `Country` | Customer's country |

The raw dataset is kept outside version control and is not included in this GitHub repository.

## Dataset Setup

This project uses the **Online Retail** dataset published by Tunguz on Kaggle.

**Dataset source:** https://www.kaggle.com/datasets/tunguz/online-retail

### Download and prepare the data

1. Open the Kaggle dataset page above.
2. Download the dataset files. Sign in to Kaggle if prompted.
3. Extract the downloaded archive if necessary.
4. Locate the CSV file used by this project, `Online_Retail.csv`.
5. Place it in the following project directory:

   `data/raw/Online_Retail.csv`

The pipeline expects the CSV file at this exact path and uses its original column names.

**Important:** The raw dataset and generated pipeline outputs are excluded from this Git repository. Anyone running the project must download the dataset separately before executing the pipeline.

Dataset attribution: Tunguz, *Online Retail*, Kaggle. See the linked dataset page for the publisher's licensing and attribution information.

## Data Quality and Cleaning

The pipeline applies the following rules when creating the cleaned transaction dataset:

- `InvoiceNo` must be present and must not start with `C`.
- `StockCode` must be present and non-empty.
- `Description` must be present and non-empty.
- `Quantity` must be greater than zero.
- `InvoiceDate` must be present and valid.
- `UnitPrice` must be greater than zero.
- `CustomerID` must be present and non-empty.
- `Country` must be present and non-empty.
- Exact duplicate rows are removed.

Rows that fail these rules are excluded from the cleaned dataset.

### Investigation Datasets

Separate datasets are generated to help investigate source-data issues:

- Missing invoice numbers
- Missing stock codes
- Missing product descriptions
- Missing quantities
- Missing invoice dates
- Missing unit prices
- Missing customer IDs
- Missing countries
- Non-positive quantities
- Non-positive unit prices
- Cancellation invoices
- Exact duplicate rows

Investigation datasets may overlap because a single record can meet more than one issue condition. The duplicate report includes all occurrences belonging to duplicate groups, rather than only one representative row.

Cancellation invoices and other non-positive values are reported separately so that their business meaning can be investigated instead of assuming every anomaly has the same cause.

The raw source dataset remains unchanged.

## Transformations and Analytics

### Revenue Calculation

The pipeline adds a calculated revenue column for each transaction line:

`CalculatedRevenue = Quantity × UnitPrice`

The result is rounded to two decimal places.

This is a calculated transaction-line value, not a confirmation of payment settlement.

### Country Summary

Groups records by country and calculates:

- Total revenue
- Transaction-line count

### Product Summary

Groups records by stock code and calculates:

- Total quantity sold
- Total revenue
- Transaction-line count

### Daily Sales Summary

Groups records by sales date and calculates:

- Total revenue
- Transaction-line count

These summaries provide a starting point for retail sales analysis and downstream reporting.

## Output Structure

The pipeline writes its output into local directories:

```text
data/
├── raw/
│   └── Online_Retail.csv
├── processed/
│   ├── cleaned_retail/
│   └── cleaned_retail_sample_csv/
├── curated/
│   ├── country_summary/
│   ├── country_summary_csv/
│   ├── product_summary/
│   ├── product_summary_csv/
│   ├── daily_sales_summary/
│   └── daily_sales_summary_csv/
└── rejected/
    ├── missing_customer_id/
    ├── cancellation_invoices/
    ├── duplicate_rows/
    └── ...
```

The structure above illustrates the main output categories; the rejected directory contains additional investigation datasets.

- **`raw/`** — original source dataset
- **`processed/`** — cleaned transaction data and an inspection-friendly CSV sample
- **`curated/`** — country, product, and daily sales summaries
- **`rejected/`** — records selected for data quality investigation

Parquet is used for primary datasets, while CSV outputs make selected results easier to inspect.

Spark writes these datasets as directories containing part files and metadata rather than necessarily producing a single file per dataset.

The pipeline currently uses overwrite mode when writing outputs. Rerunning it replaces existing output datasets.

## Amazon S3 Storage

Amazon S3 is used as the cloud object storage layer for the project.

The raw dataset and local pipeline outputs are uploaded separately using the AWS CLI. The S3 layout follows the same logical separation as the local output directories:

```text
s3://<your-bucket>/
├── raw/
│   └── Online_Retail.csv
├── processed/
├── curated/
└── rejected/
```

Example commands for uploading the generated outputs:

```bash
aws s3 sync data/processed/ s3://<your-bucket>/processed/ --profile retail-lake-dev

aws s3 sync data/curated/ s3://<your-bucket>/curated/ --profile retail-lake-dev

aws s3 sync data/rejected/ s3://<your-bucket>/rejected/ --profile retail-lake-dev
```

The raw dataset can be uploaded separately:

```bash
aws s3 cp data/raw/Online_Retail.csv s3://<your-bucket>/raw/Online_Retail.csv --profile retail-lake-dev
```

Replace `<your-bucket>` with your configured S3 bucket name.

To inspect the uploaded objects:

```bash
aws s3 ls s3://<your-bucket>/ --recursive --summarize --profile retail-lake-dev
```

AWS credentials are managed through a local AWS CLI profile and are not stored in the repository.

**Important:** These are separate AWS CLI operations. Running `python src/main.py` does not upload outputs to S3 automatically.

## Automated Testing

The project includes pytest tests covering important cleaning rules, including:

- Cleaned records have positive quantities and unit prices.
- Records with missing customer IDs are excluded from the cleaned dataset.

Run the tests from the project root:

```bash
python -m pytest -v
```

The existing validation tests passed during development.

## Project Setup

### Prerequisites

- Python 3.12 or a compatible Python version
- A Java runtime compatible with the installed PySpark version
- AWS CLI configured with access to the target S3 bucket if you want to perform S3 uploads

### 1. Clone the repository

```bash
git clone https://github.com/Kailash-x1251x/online-retail-data-lake.git
cd online-retail-data-lake
```

### 2. Create and activate a virtual environment

```bash
python3 -m venv .venv
source .venv/bin/activate
```

### 3. Install dependencies

Once the project's dependency file is created, install the listed packages using:

```bash
pip install -r requirements.txt
```

### 4. Add the source dataset

Place the Online Retail CSV at:

```text
data/raw/Online_Retail.csv
```

The dataset is not included in the GitHub repository and must be obtained separately.

### 5. Run the pipeline

From the project root, execute:

```bash
python src/main.py
```

The pipeline reads the local CSV, processes the data, generates summaries and investigation datasets, and writes the outputs locally.

### 6. Run the tests

```bash
python -m pytest -v
```

### 7. Upload outputs to S3

Configure your AWS CLI profile and bucket access, then run the AWS CLI sync commands described in the Amazon S3 section.

## Limitations and Future Improvements

- Spark runs locally rather than on a managed or multi-node cluster.
- Outputs currently use overwrite mode; incremental processing and change detection are not implemented.
- S3 uploads are manual CLI operations rather than an automated pipeline stage.
- Additional tests could cover transformation logic, investigation datasets, and output generation.
- Logging, configuration management, and failure recovery could be expanded.
- Workflow orchestration could be added in a future version.

## Learning Outcomes

This project provided hands-on experience with:

- Schema-based data ingestion using PySpark
- Data quality validation and record filtering
- Duplicate detection and investigation datasets
- Revenue calculations and analytical aggregations
- Parquet and CSV output generation
- Automated tests using pytest
- Amazon S3 storage and AWS CLI operations
- Git-based version control and project documentation

