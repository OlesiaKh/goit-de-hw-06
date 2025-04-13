from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, to_timestamp, expr, to_json
from pyspark.sql.types import StructType, StringType, DoubleType

# 1. Створення Spark-сесії
spark = SparkSession.builder \
    .appName("Streaming IoT Alerts") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5") \
    .getOrCreate()

# 2. Опис схеми Kafka повідомлень
schema = StructType() \
    .add("sensor_id", StringType()) \
    .add("timestamp", StringType()) \
    .add("temperature", DoubleType()) \
    .add("humidity", DoubleType())

# 3. Зчитування даних з Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "building_sensors_olesia") \
    .load()

# 4. Декодування JSON
json_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# 5. Конвертація до timestamp
stream_df = json_df.withColumn("timestamp", to_timestamp("timestamp"))

# 6. Агрегація через sliding window
agg_df = stream_df \
    .withWatermark("timestamp", "10 seconds") \
    .groupBy(
        window(col("timestamp"), "1 minute", "30 seconds"),
        col("sensor_id")
    ) \
    .agg(
        avg("temperature").alias("avg_temp"),
        avg("humidity").alias("avg_hum")
    )

# 7. Читання умов для алертів з CSV
alerts_df = spark.read.csv("alerts.conditions.csv", header=True, inferSchema=True)

# 8. Перехресне з'єднання і фільтрація
joined_df = agg_df.crossJoin(alerts_df).filter(
    ((col("temperature_min") != -999) & (col("avg_temp") < col("temperature_min"))) |
    ((col("temperature_max") != -999) & (col("avg_temp") > col("temperature_max"))) |
    ((col("humidity_min") != -999) & (col("avg_hum") < col("humidity_min"))) |
    ((col("humidity_max") != -999) & (col("avg_hum") > col("humidity_max")))
)

# 9. Вивід у консоль (для тесту)
query_to_console = joined_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# 10. Запис у Kafka-топік alerts_olesia
query_to_kafka = joined_df \
    .selectExpr("CAST(sensor_id AS STRING) AS key", "to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "alerts_olesia") \
    .option("checkpointLocation", "chk-point-dir") \
    .outputMode("append") \
    .start()

query_to_kafka.awaitTermination()
query_to_console.awaitTermination()


