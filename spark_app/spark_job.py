from pyspark.sql import SparkSession
from pyspark.sql.functions import col, get_json_object, from_json
from pyspark.sql.types import StringType, StructType, StructField

# Sesja
spark = SparkSession.builder.appName("Kafka All Tables Stream").getOrCreate()

# Stream z Kafki
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribePattern", "debezium.public.*") \
    .option("startingOffsets", "earliest") \
    .load()

# Parsowanie wartości
json_df = df.selectExpr("CAST(value AS STRING) as json_str") \
    .withColumn("after", get_json_object(col("json_str"), "$.payload.after")) \
    .filter(col("after").isNotNull())

# Przykładowy schemat (dla jednej tabeli)
schema = StructType([
    StructField("id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True)
])

parsed_df = json_df.withColumn("after_json", from_json(col("after"), schema)) \
                   .select("after_json.*")

# Konsola
query1 = json_df.select("after") \
    .writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", True) \
    .option("numRows", 10) \
    .start()

# Delta Lake
query2 = parsed_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "s3a://minio-bucket/_checkpoints/debezium-data") \
    .option("path", "s3a://minio-bucket/debezium-data") \
    .start()

# Poprawne zatrzymanie
spark.streams.awaitAnyTermination()



