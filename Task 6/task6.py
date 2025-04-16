import sys, string
import os
import socket
import time
import operator
import boto3
import json
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.window import Window
from pyspark.sql.functions import from_unixtime, date_format, month, expr, sum, avg
from pyspark.sql.functions import to_date, count, col, row_number
from graphframes import *



if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("TestDataset")\
        .getOrCreate()
    
    # shared read-only object bucket containing datasets
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

    # 1st join operation
    rideshare_data_with_pickup_df = rideshare_data_df.join(taxi_zone_lookup_df,rideshare_data_df.pickup_location ==  taxi_zone_lookup_df.LocationID,"inner")\
        .withColumnRenamed("Borough","Pickup_Borough").withColumnRenamed("Zone","Pickup_Zone").withColumnRenamed("service_zone","Pickup_service_zone").drop("LocationID")
    # 2nd join operation
    rideshare_df = rideshare_data_with_pickup_df.join(taxi_zone_lookup_df,rideshare_data_with_pickup_df.dropoff_location ==  taxi_zone_lookup_df.LocationID,"inner")\
        .withColumnRenamed("Borough","Dropoff_Borough").withColumnRenamed("Zone","Dropoff_Zone").withColumnRenamed("service_zone","Dropoff_service_zone").drop("LocationID")
    # Changing date from UNIX timestamp to 'yyyy-MM-dd' format
    rideshare_df = rideshare_df.withColumn("date", from_unixtime(col("date"),"yyyy-MM-dd"))

    # Task 6
    # Selecting the required columns, grouping by "Pickup_Borough" and "time_of_day"
    tripcount_df = rideshare_df.select(rideshare_df.Pickup_Borough,rideshare_df.time_of_day)
    tripcount_df = tripcount_df.groupBy("Pickup_Borough", "time_of_day").count().withColumnRenamed("count","trip_count")
    # Filter the data to get 0 < trip_count < 1000, and printing the dataframe
    tripcount_df_1 = tripcount_df.where((tripcount_df["trip_count"] > 0) & (tripcount_df["trip_count"] < 1000))
    tripcount_df_1.show()

    # Filter the data to get trips in evening time, and print the dataframe
    tripcount_df_2 = tripcount_df.where(tripcount_df["time_of_day"] == "evening")
    tripcount_df_2.show()

    # Select the required dataframe, and filter the data to get pickup_borough = Brooklyn and dropoff_borough = Staten Island
    pickup_dropoff_df = rideshare_df.select(rideshare_df.Pickup_Borough,rideshare_df.Dropoff_Borough,rideshare_df.Pickup_Zone)
    pickup_dropoff_df = pickup_dropoff_df.where((pickup_dropoff_df["Pickup_Borough"] == "Brooklyn") & (pickup_dropoff_df["Dropoff_Borough"] == "Staten Island"))
    pickup_dropoff_df.show(n=10,truncate=False)
    trip_number = pickup_dropoff_df.count()
    print(f"Number of trips = {trip_number}")
    
    
    spark.stop()