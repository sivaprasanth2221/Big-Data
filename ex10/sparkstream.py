from pyspark.sql import SparkSession
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import col, split

# Initialize Spark session
spark = SparkSession.builder.appName("KafkaSparkCSVStreaming").getOrCreate()

# Read streaming data from Kafka
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "test-stream") \
    .load()

# Kafka data comes as key-value pairs, we extract the value (CSV string)
df = df.selectExpr("CAST(value AS STRING)")

# Split the CSV values into columns (assuming CSV structure: feature1, feature2, label)
df = df.withColumn("value", split(col("value"), ","))

df = df.select(
    col("value").getItem(0).cast("double").alias("feature1"),
    col("value").getItem(1).cast("double").alias("feature2"),
    col("value").getItem(2).cast("double").alias("label")
)

# VectorAssembler for features
assembler = VectorAssembler(inputCols=["feature1", "feature2"], outputCol="features")
df = assembler.transform(df)

# Memory sink to accumulate the data
query = df.writeStream.format("memory").queryName("streaming_data").outputMode("append").start()

# Let the stream run for a while to collect data
import time
time.sleep(20)  # Adjust time as needed to accumulate enough data

# Now, fetch the accumulated data from memory
static_df = spark.sql("SELECT * FROM streaming_data")

# Logistic Regression for Classification
lr = LogisticRegression(featuresCol="features", labelCol="label")
lr_model = lr.fit(static_df)

# Perform classification prediction
classification_predictions = lr_model.transform(static_df)

# KMeans Clustering
kmeans = KMeans(k=3, featuresCol="features")
kmeans_model = kmeans.fit(static_df)

# Perform clustering prediction
clustering_predictions = kmeans_model.transform(static_df)

# Show the results
classification_predictions.show()
clustering_predictions.show()

# Stop the query once done
query.stop()
