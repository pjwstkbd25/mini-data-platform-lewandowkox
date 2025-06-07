from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType


spark = SparkSession.builder \
    .appName("Kafka JSON Stream") \
    .getOrCreate()

# Schemat danych (dopasowany do danych z topiku products)
schema = StructType([
    StructField("product_id", LongType()),
    StructField("name", StringType()),
    StructField("category", StringType()),
    StructField("price", DoubleType()),
    StructField("stock_quantity", LongType()),
    StructField("created_at", StringType())
])

# Odczyt z Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "debezium.public.products") \
    .option("startingOffsets", "latest") \
    .load()

# Deserializacja JSON
json_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Prosta transformacja
result = json_df.select("name", "price", "stock_quantity")

# Wypisanie do konsoli
query = result.writeStream \
    .format("console") \
    .outputMode("append") \
    .start()

query.awaitTermination()
