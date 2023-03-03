import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

from dotenv import load_dotenv
load_dotenv()

user=os.environ.get('user')
passwrd=os.environ.get('password')

# Download spark sql kakfa package from Maven repository and submit to PySpark at runtime. 
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1,org.postgresql:postgresql:42.5.1 pyspark-shell'
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars postgresql-42.2.18.jar pyspark-shell'

# specify the topic we want to stream data from.
kafka_topic_name = "pinterest"
# Specify your Kafka server to read data from.
kafka_bootstrap_servers = 'localhost:9092'

spark = SparkSession \
        .builder \
        .appName("KafkaStreaming ") \
        .getOrCreate()

# Only display Error messages in the console.
spark.sparkContext.setLogLevel("ERROR")

# Construct a streaming DataFrame that reads from topic
stream_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic_name) \
        .option("startingOffsets", "earliest") \
        .load()

# Select the value part of the kafka message and cast it to a string.
stream_df = stream_df.selectExpr("CAST(value as STRING)")
# Define the schema for the JSON data
json_schema = StructType([
    StructField("unique_id", StringType(), True),
    StructField("title", StringType(), True),
    StructField("follower_count", StringType(), True)
])

# Apply the schema to the JSON data and select only the relevant columns
transformed_df = stream_df.select(from_json(col("value"), json_schema).alias("data")) \
        .select("data.*")

# Set the JDBC driver class name and database URL
jdbc_driver_class = "org.postgresql.Driver"
jdbc_url = "jdbc:postgresql://pinterest.crk5rug4p4jc.us-east-1.rds.amazonaws.com:5432/postgres"

# Set the username and password for the RDS instance
pg_properties = {
    "user": {user},
    "password": {passwrd}
}

# Write the transformed data to RDS using the JDBC connector
def write_to_rds(df, epoch_id):
    df.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("driver", jdbc_driver_class) \
        .option("dbtable", "experimental_data") \
        .options(**pg_properties) \
        .mode("append") \
        .save()

transformed_df.writeStream \
    .foreachBatch(write_to_rds) \
    .start() \
    .awaitTermination()
