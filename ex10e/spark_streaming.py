from pyspark.sql import SparkSession
from pyspark.ml.regression import LinearRegression
from pyspark.ml.recommendation import ALS
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import col, split
import time

# Initialize Spark session with Kafka package
spark = SparkSession.builder \
    .appName("KafkaSparkCSVStreaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1") \
    .getOrCreate()

# Read streaming data from Kafka
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "data-stream") \
    .load()

# Kafka data comes as key-value pairs, we extract the value (CSV string)
df = df.selectExpr("CAST(value AS STRING)")

# Split the CSV values into columns
df = df.withColumn("value", split(col("value"), ","))

# Select and cast the appropriate columns
df = df.select(
    col("value").getItem(0).cast("int").alias("userId"),
    col("value").getItem(1).cast("int").alias("itemId"),
    col("value").getItem(2).cast("double").alias("rating"),
    col("value").getItem(3).cast("double").alias("feature1"),
    col("value").getItem(4).cast("double").alias("feature2"),
    col("value").getItem(5).cast("double").alias("label")
)

# Memory sink to accumulate the data
query = df.writeStream.format("memory").queryName("streaming_data").outputMode("append").start()

# Let the stream run for a while to collect data
time.sleep(20)  # Adjust time as needed to accumulate enough data

# Now, fetch the accumulated data from memory
static_df = spark.sql("SELECT * FROM streaming_data")

# Prepare data for Linear Regression
lr_data = static_df.select("feature1", "feature2", "label").na.drop()  # Drop rows with nulls

# VectorAssembler for features
assembler = VectorAssembler(inputCols=["feature1", "feature2"], outputCol="features")
lr_data = assembler.transform(lr_data)

# Create and fit the Linear Regression model
lr = LinearRegression(featuresCol="features", labelCol="label")
lr_model = lr.fit(lr_data)

# Generate predictions from the Linear Regression model
lr_predictions = lr_model.transform(lr_data)

# Display the Linear Regression Predictions
print("Linear Regression Predictions:")
lr_predictions.select("features", "label", "prediction").show()

# Prepare data for Collaborative Filtering with ALS
# Ensure that we drop any rows with nulls in the relevant columns
als_data = static_df.select("userId", "itemId", "rating").na.drop()

# Create and fit the ALS model
als = ALS(maxIter=10, regParam=0.01, userCol="userId", itemCol="itemId", ratingCol="rating", coldStartStrategy="drop")
als_model = als.fit(als_data)

# Generate recommendations for all users
user_recs = als_model.recommendForAllUsers(3)  # Recommend 3 items for each user

# Show the User Recommendations
print("User Recommendations:")
user_recs.show()

# Stop the query once done
query.stop()

