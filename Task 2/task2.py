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


    # Task 2
    # Selecting the required columns
    business_month_df = rideshare_df.select(rideshare_df.business, rideshare_df.date, rideshare_df.rideshare_profit.cast("int"), rideshare_df.driver_total_pay.cast("int"))
    # creating a new column "Month" by extracting the month element from the "date" column
    business_month_df = business_month_df.withColumn("Month", month(business_month_df["date"]))
    # concatenating the "business" column and "month" column
    business_month_df = business_month_df.withColumn("business_month",expr(" business ||'-'|| Month"))

    # Grouping the dataframe by business-month and getting the count of each group
    business_month_count_df = business_month_df.groupBy("business_month").count()

    # Grouping the dataframe by business-month and getting the sum of rideshare profit for each group and renaming the column to "profit"
    business_month_profit_df = business_month_df.groupBy("business_month").agg(sum("rideshare_profit").alias("profit"))

    # Grouping the dataframe by business-month and getting the sum of driver total pay for each group and renaming the column to "driver_earning"
    business_month_driverEarning_df = business_month_df.groupBy("business_month").agg(sum("driver_total_pay").alias("driver_earning"))

    # Writing the 3 dataframes into CSV files and storing them in personal bucket
    business_month_count_df.coalesce(1).write.options(header=True).csv("s3a://" + s3_bucket + "/task2/q1")
    business_month_profit_df.coalesce(1).write.options(header=True).csv("s3a://" + s3_bucket + "/task2/q2")
    business_month_driverEarning_df.coalesce(1).write.options(header=True).csv("s3a://" + s3_bucket + "/task2/q3")
    
    spark.stop()