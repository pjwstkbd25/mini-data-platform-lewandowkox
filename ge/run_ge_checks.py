import os
import tempfile
from pathlib import Path

import boto3
import pandas as pd
import great_expectations as gx

# MinIO / S3 settings (Docker Desktop Mac)
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://host.docker.internal:9100")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")

BUCKET = os.environ.get("MINIO_BUCKET", "lake")
SILVER_PREFIX = os.environ.get("SILVER_PREFIX", "silver/orders_delta/")

def download_one_parquet_from_minio() -> Path:
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1",
    )

    resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=SILVER_PREFIX)
    contents = resp.get("Contents", [])
    parquet_keys = [o["Key"] for o in contents if o["Key"].endswith(".parquet")]

    if not parquet_keys:
        raise FileNotFoundError(
            f"No .parquet objects found in s3://{BUCKET}/{SILVER_PREFIX} "
            f"(endpoint={MINIO_ENDPOINT})"
        )

    key = parquet_keys[0]
    tmpdir = Path(tempfile.mkdtemp(prefix="silver_ge_"))
    local_path = tmpdir / Path(key).name
    s3.download_file(BUCKET, key, str(local_path))
    return local_path

def main():
    parquet_path = download_one_parquet_from_minio()
    df = pd.read_parquet(parquet_path)

    context = gx.get_context()

    # idempotentnie (można odpalać wiele razy)
    datasource = context.sources.add_or_update_pandas(name="silver_pandas")
    asset = datasource.add_dataframe_asset(name="orders_silver")

    batch_request = asset.build_batch_request(dataframe=df)
    validator = context.get_validator(batch_request=batch_request)

    # MVP expectations (zmień kolumnę jeśli u Ciebie inna)
    # 1) minimalna liczba rekordów
    validator.expect_table_row_count_to_be_between(min_value=1)

    # 2) klucz główny nie może mieć nulli
    validator.expect_column_values_to_not_be_null("order_id")

    # 3) klucz główny ma być unikalny
    validator.expect_column_values_to_be_unique("order_id")

    result = validator.validate()
    print("GE validation success:", result.success)

    # opcjonalnie - budowa docs
    context.build_data_docs()

if __name__ == "__main__":
    main()
