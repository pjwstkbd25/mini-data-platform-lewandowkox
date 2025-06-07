from pyspark.sql import SparkSession
from pyspark.sql.functions import col, get_json_object

spark = SparkSession.builder \
    .appName("Kafka All Tables Stream") \
    .getOrCreate()

# Odczyt z Kafki – subskrybujemy WSZYSTKIE topiki z tabel Debezium
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribePattern", "debezium.public.*") \
    .option("startingOffsets", "latest") \
    .load()

# Parsujemy tylko JSON i wyciągamy payload.after jako tekst
json_df = df.selectExpr("CAST(value AS STRING) as json_str") \
    .withColumn("after", get_json_object(col("json_str"), "$.payload.after"))

# Wyświetlamy cały "after" jako string (czyli zawartość zmienionego rekordu)
query = json_df.select("after") \
    .writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
