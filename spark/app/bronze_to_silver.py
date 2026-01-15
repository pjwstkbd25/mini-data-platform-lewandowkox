import os
from pyspark.sql import SparkSession, functions as F

S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")

BUCKET = os.getenv("S3_BUCKET", "lake")
BRONZE = os.getenv("S3_BRONZE_PREFIX", "bronze")
SILVER = os.getenv("S3_SILVER_PREFIX", "silver")

def s3url(path: str) -> str:
    return f"s3a://{BUCKET}/{path}"

def build_spark():
    spark = SparkSession.builder.appName("BronzeToSilver").getOrCreate()
    hconf = spark._jsc.hadoopConfiguration()
    hconf.set("fs.s3a.endpoint", S3_ENDPOINT)
    hconf.set("fs.s3a.access.key", AWS_ACCESS_KEY_ID)
    hconf.set("fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
    hconf.set("fs.s3a.path.style.access", "true")
    hconf.set("fs.s3a.connection.ssl.enabled", "false")
    return spark

def main():
    spark = build_spark()

    # MVP: na start zakładamy, że wrzucisz plik do MinIO: lake/bronze/orders.csv
    bronze_path = s3url(f"{BRONZE}/orders.csv")
    df = spark.read.option("header", True).csv(bronze_path)

    # minimalne czyszczenie (MVP)
    df2 = df.dropna()

    out_path = s3url(f"{SILVER}/orders_delta")
    df2.write.mode("overwrite").format("delta").save(out_path)

    print(f"✅ Silver zapisany do: {out_path}")
    spark.stop()

if __name__ == "__main__":
    main()
