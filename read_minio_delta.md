# Retrieving Data from MinIO using Apache Spark and Delta Lake

This document explains how to access and read data stored in MinIO in Delta Lake format using Apache Spark.

---

## ✅ Prerequisites

1. MinIO is up and running (as defined in `docker-compose.yml`).
2. Delta-formatted data is already written by Spark to MinIO (e.g. `s3a://minio-bucket/debezium-data`).
3. Your Spark job is configured with the following:

   * Delta Lake
   * Hadoop AWS connector
   * S3A configuration

---

## 📦 Required Spark Packages

Make sure the following Spark packages are included (already in your docker image command):

* `io.delta:delta-spark_2.12:3.0.0`
* `org.apache.hadoop:hadoop-aws:3.3.4`

---

## 🛠 Spark Configuration Example

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Read Delta from MinIO") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()
```

---

## 📖 Reading Delta Table from MinIO

```python
df = spark.read.format("delta").load("s3a://minio-bucket/debezium-data")
df.show(truncate=False)
```

---

## 📁 Optional: Listing Available Files

You can also verify files via MinIO web UI at [http://localhost:9101](http://localhost:9101)
Navigate to the `minio-bucket` > `debezium-data` to confirm `.parquet` and `_delta_log` files exist.

---

## 🧹 Cleaning up

To restart from scratch:

```bash
docker-compose down -v
docker-compose up --build
```

---

## 📌 Notes

* You can use `filter()`, `select()`, `groupBy()` on the `df` just like any Spark DataFrame.
* This setup is ready for production-like workflows with streaming Delta updates and S3-like storage.
