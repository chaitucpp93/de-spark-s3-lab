from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator


PROJECT_ROOT = Path(__file__).resolve().parent.parent
SPARK_JOB = PROJECT_ROOT / "spark_s3_sales.py"
ENV_FILE = PROJECT_ROOT / ".env"

default_args = {
    "owner": "chaitanya",
}

with DAG(
    dag_id="spark_s3_sales_pipeline",
    description="Runs the Spark sales aggregation from S3",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
) as dag:

    run_spark_s3_sales = BashOperator(
        task_id="run_spark_s3_sales",
        retries=1,
        retry_delay=timedelta(minutes=5),
        bash_command=fr"""
set -euo pipefail

source "$HOME/airflow_env_312/bin/activate"

set -a
source "{ENV_FILE}"
set +a

credentials="$(aws configure export-credentials --profile "$AWS_PROFILE" --format env)"
eval "$credentials"

spark-submit --master "local[*]" \
  --packages org.apache.hadoop:hadoop-aws:3.5.0 \
  "{SPARK_JOB}"
""",
    )

    verify_s3_output = BashOperator(
        task_id="verify_s3_output",
        retries=1,
        retry_delay=timedelta(minutes=5),
        bash_command=fr"""
set -euo pipefail

set -a
source "{ENV_FILE}"
set +a

aws s3api head-object \
  --bucket "$S3_BUCKET" \
  --key processed/spark_sales_by_region/_SUCCESS \
  --profile "$AWS_PROFILE"

aws s3 ls \
  "s3://$S3_BUCKET/processed/spark_sales_by_region/" \
  --profile "$AWS_PROFILE" | grep 'part-' > /dev/null

echo "Verified _SUCCESS and a Spark part file."
""",
    )

    run_spark_s3_sales >> verify_s3_output