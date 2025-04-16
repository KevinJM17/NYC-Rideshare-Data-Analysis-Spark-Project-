import sys, string
import os
import socket
import time
import operator
import boto3
import json
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import from_unixtime, date_format
from pyspark.sql.functions import to_date, count, col , month
from graphframes import *
from datetime import datetime
from pyspark.sql.functions import month
from pyspark.sql.functions import col
from pyspark.sql.functions import dayofmonth, avg, to_date, month
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Define vertexSchema for vertices DataFrame
vertexSchema = StructType([
    StructField("id", IntegerType(), True),
    StructField("Borough", StringType(), True),
    StructField("Zone", StringType(), True),
    StructField("service_zone", StringType(), True)
])

# Define edgeSchema for edges DataFrame
edgeSchema = StructType([
    StructField("src", IntegerType(), True),
    StructField("dst", IntegerType(), True)
])

def convertTime(x):
    return time.strftime("%d-%b-%Y", time.gmtime(int(x)))
    
if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("TestDataset")\
        .getOrCreate()
    
    s3_data_repository_bucket = os.environ['DATA_REPOSITORY_BUCKET']

    s3_endpoint_url = os.environ['S3_ENDPOINT_URL']+':'+os.environ['BUCKET_PORT']
    s3_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    s3_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    s3_bucket = os.environ['BUCKET_NAME']

    hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.endpoint", s3_endpoint_url)
    hadoopConf.set("fs.s3a.access.key", s3_access_key_id)
    hadoopConf.set("fs.s3a.secret.key", s3_secret_access_key)
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")

    # Task 1
    # Loading the data to dataframes
    rideshare_data_df = spark.read.option("header", True).csv("s3a://" + s3_data_repository_bucket + "/ECS765/rideshare_2023/rideshare_data.csv")        # /read from the bucket file
    taxi_zone_lookup_df = spark.read.option("header", True).csv("s3a://" + s3_data_repository_bucket + "/ECS765/rideshare_2023/taxi_zone_lookup.csv")    # /read from the bucket file

    # Construct edges DataFrame
    edges_df = rideshare_data_df.select("pickup_location", "dropoff_location").toDF("src", "dst")

    # Construct vertices DataFrame
    vertices_df = taxi_zone_lookup_df.select("LocationID", "Borough", "Zone", "service_zone").toDF("id", "Borough", "Zone", "service_zone")

    # Printing the vertices and edges tables
    vertices_df.show(n=10,truncate=False)
    edges_df.show(10)
    
    # Create a graph using vertices and edges
    graph = GraphFrame(vertices_df, edges_df)
    # Print 10 samples of the graph DataFrame
    graph.triplets.show(n=10,truncate=False)

    
    # Count connected vertices with the same Borough and service_zone
    connected_counts = graph.find("(a)-[e]->(b)").filter("a.Borough = b.Borough AND a.service_zone = b.service_zone")
    connected_counts.select(col("a.id").alias("id_src"),col("b.id").alias("id_dst"),"a.Borough","a.service_zone").show(10, truncate=False)
    counts = connected_counts.count()
    print(f"Count = {counts}")


    # Page ranking on the graph DataFrame
    pagerank_results = graph.pageRank(resetProbability=0.17, tol=0.01).vertices.select("id", "pagerank").orderBy(col("pagerank").desc()).show(n=5,truncate=False)

    spark.stop()
