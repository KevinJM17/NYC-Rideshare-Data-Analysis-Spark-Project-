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

    # Task 4
    # Seleting the required columns
    driver_pay_trip_df = rideshare_df.select(rideshare_df.time_of_day,rideshare_df.driver_total_pay,rideshare_df.trip_length)

    # Grouping the dataframe by "time_of_day", getting the average of driver total pay, and printing the resulting dataframe
    avg_driver_pay_df = driver_pay_trip_df.groupBy("time_of_day").agg(avg("driver_total_pay").alias("average_drive_total_pay")).drop("driver_total_pay")
    avg_driver_pay_df.show()

    # Grouping the dataframe by "time_of_day", getting the average of trip lenght, and printing the resulting dataframe
    avg_trip_len_df = driver_pay_trip_df.groupBy("time_of_day").agg(avg("trip_length").alias("average_trip_length")).drop("trip_length")
    avg_trip_len_df.show()

    # Joining the 2 dataframes by their common column (time_of_day), calculating the average earning per mile by dividing "average_drive_total_pay" and "average_trip_length", and printing the resulting dataframe
    avg_earning_per_mile_df = avg_driver_pay_df.join(avg_trip_len_df,"time_of_day")
    avg_earning_per_mile_df = avg_earning_per_mile_df.withColumn("average_earning_per_mile",col("average_drive_total_pay")/col("average_trip_length"))\
        .drop("average_drive_total_pay","average_trip_length")
    avg_earning_per_mile_df.show()
    
    
    spark.stop()