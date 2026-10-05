# AWS S3 + PySpark Sales Pipeline

## Goal

Read sales CSV data from Amazon S3, aggregate sales by region with PySpark, write the result back to S3, and use Airflow to run and verify the pipeline.

## Pipeline flow

1. A manually triggered Airflow DAG loads local configuration and exports temporary AWS credentials.
2. `spark-submit` loads `org.apache.hadoop:hadoop-aws:3.5.0`, which provides the S3A connector and its dependencies.
3. Spark reads `raw/sales_data.csv` from S3 through S3A.
4. Spark groups the records by region, sums sales, and writes CSV output under `processed/spark_sales_by_region/`.
5. Airflow verifies that the output contains both `_SUCCESS` and a Spark part file.

## Prerequisites

- Ubuntu on WSL2
- Python 3.12 and PySpark 4.2.0
- Java 17
- AWS CLI v2
- Airflow 2.11.0
- AWS access configured through AWS CLI profiles

## Configuration

Copy `.env.example` to `.env`:

```bash
cp .env.example .env
```

Edit `.env` to match your AWS setup:

```text
S3_BUCKET=your-bucket-name
AWS_PROFILE=your-s3-profile
AWS_REGION=ap-south-1
```

The `.env` file is local configuration and is excluded from Git. Do not put AWS credentials in it; credentials come from your AWS CLI profile.

## Authenticate and upload the sample input

From the project directory in WSL, load the configuration:

```bash
set -a
source .env
set +a
```

If your AWS login session has expired, refresh it using the profile that holds your login credentials. In Chaitanya's setup, that profile is `de-lab-user`:

```bash
aws login --profile de-lab-user --region "$AWS_REGION" --remote
```

Confirm the S3 profile's identity:

```bash
aws sts get-caller-identity --profile "$AWS_PROFILE"
```

Upload the included sample CSV to the input location:

```bash
aws s3 cp sales_data_original.csv \
  "s3://${S3_BUCKET}/raw/sales_data.csv" \
  --profile "$AWS_PROFILE"
```

## Data locations

- Input CSV: `s3a://${S3_BUCKET}/raw/sales_data.csv`
- Output prefix: `s3a://${S3_BUCKET}/processed/spark_sales_by_region/`

## Run the pipeline

With Airflow running, manually trigger `spark_s3_sales_pipeline` in the Airflow UI. The DAG has no automatic schedule.

Confirm that both `run_spark_s3_sales` and `verify_s3_output` succeed.

## Transformation and output

The Spark job groups records by `region` and calculates `total_sales` by summing `sales`.

Spark writes CSV part files under the output prefix. It also writes `_SUCCESS` when the output write completes.