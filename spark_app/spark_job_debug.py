from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Inicjalizacja SparkSession
spark = SparkSession.builder \
    .appName("KafkaDebugReader") \
    .getOrCreate()

# Wczytanie strumienia z Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribePattern", "debezium.public.*") \
    .option("startingOffsets", "earliest") \
    .load()

# Konwersja z bajtów na string
json_df = df.selectExpr("CAST(value AS STRING) as json_str")

# Wypisanie danych do konsoli
query = json_df.writeStream \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
