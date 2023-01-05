from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import *
from pyspark.sql.window import Window

from dotenv import load_dotenv
from json import load 
import os
import json
import multiprocessing
import pyspark
import findspark
import boto3
load_dotenv()

os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages com.amazonaws:aws-java-sdk-s3:1.12.196,org.apache.hadoop:hadoop-aws:3.3.1 pyspark-shell"



class spark_transformation:
    def __init__(self):
        self.aws_access_key_id=os.environ.get('access_keys')
        self.aws_secret_access_key=os.environ.get('secret_key')
        self.findspark=findspark.init(os.environ["SPARK_HOME"])
        self.s3 = boto3.client("s3",aws_access_key_id=self.aws_access_key_id, aws_secret_access_key=self.aws_secret_access_key)
        self.cfg = (
            pyspark.SparkConf()
            # Setting the master to run locally and with the maximum amount of cpu coresfor multiprocessing.
            .setMaster(f"local[{multiprocessing.cpu_count()}]")
            # Setting application name
            .setAppName("TestApp")
            # Setting config value via string
            .set("spark.eventLog.enabled", False)
            # Setting environment variables for executors to use
            .setExecutorEnv(pairs=[("VAR3", "value3"), ("VAR4", "value4")])
            # Setting memory if this setting was not set previously
            .setIfMissing("spark.executor.memory", "1g")
        )
        
        

    def spark_sessions(self):
        # Starting the SparkSession
        self.sc = SparkContext(conf = self.cfg)
        self.spark = SparkSession(self.sc)

    def count_files(self):
        # Set the name of the bucket that you want to count the files in
        bucket_name = 'pinterestmax'

        # Use the S3 client to list the objects in the bucket
        objects = self.s3.list_objects(Bucket=bucket_name)

        # Get the list of objects from the response
        object_list = objects['Contents']
        # Initialize a counter for the number of files
        file_count = 0

        # Iterate through the list of objects
        for obj in object_list:
            # Get the object key (i.e., the file name)
            key = obj['Key']
            # Check if the key ends with a file extension (e.g., '.txt', '.pdf', etc.)
            if key.endswith(('.json')):
                # Increment the file count
                file_count += 1
        
        return file_count

    def create_resource(self):
        counting=self.count_files()
        # Set the name of the bucket and key for the file you want to get
        bucket_name = 'pinterestmax'
        key = 'stored_data_0.json'
        # Use the S3 client to get the file from S3 and save it to a variable
        s3_file = self.s3.get_object(Bucket=bucket_name, Key=key)
        # Get the body of the file as a JSON object
        json_data = json.loads(s3_file['Body'].read())
        #Convert the JSON data to a Spark DataFrame
        df = self.spark.createDataFrame([list(json_data.values())], list(json_data.keys()))
        

        prefix = ''
        objects = self.s3.list_objects(Bucket=bucket_name, Prefix=prefix, Marker='1', MaxKeys=counting)
        

        #Iterate over the objects and append their data to the file_data list
        for obj in objects['Contents']:
            key = obj['Key']
            body = self.s3.get_object(Bucket=bucket_name, Key=key)['Body'].read()
            body = body.decode('utf-8-sig')
            rest_of_data = json.loads(body)
            df_read = self.spark.createDataFrame([list(rest_of_data.values())], list(rest_of_data.keys()))
            self.df = df.union(df_read)

        return self.df
            
    def transformations(self):
        dft = self.df.withColumnRenamed("title", "Title_max")
        # Replace null values with 0
        dft = dft.na.fill(0)
        # Add rank column
        dft = dft.withColumn("rank", row_number().over(Window.orderBy(asc("index"))))
        dft.show(10)


        
        

        

        




    def run_prog(self):
        self.spark_sessions()
        self.create_resource()
        self.transformations()
        self.sc.stop()

if __name__ == "__main__":
    spark_transformation().run_prog()
