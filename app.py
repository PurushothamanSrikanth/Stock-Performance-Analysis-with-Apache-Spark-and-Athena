#Importing all necessary libraries

import pandas, numpy, os
import pyspark
from pyspark import SparkConf, SparkContext
from pyspark.sql import *
import boto3
import findspark
findspark.init()
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType

#Creating a Spark Session

conf = SparkConf() \
    .setAppName("Purush_ETL")

conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.2.0')
conf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider')
 
spark = SparkSession \
    .builder \
    .config(conf = conf) \
    .config("spark.dynamicAllocation.enabled", "true") \
    .enableHiveSupport() \
    .getOrCreate()

S3_BUCKET_PATH = "s3a://purushstockdata/data/"


## Reading the data from S3

### Reading the Metadata

#Schema for symbol metadata
sym_meta_schema = StructType([ \
    StructField("Symbol", StringType(), True), \
    StructField("Name", StringType(), True), \
    StructField("Country", StringType(), True), \
    StructField("Sector", StringType(), True), \
    StructField("Industry", StringType(), True), \
    StructField("Address", StringType(), True) \
])

sym_meta = spark.read.option("header", True).schema(sym_meta_schema).csv(S3_BUCKET_PATH+"symbol_metadata.csv")

sym_meta.printSchema() #check the Schema of the dataframe
sym_meta.show()

## Before reading the Stock data, we need to make sure we read the Stock data  
## only for those companies/symbols that are listed in the Metadata file.
sym_list = sym_meta.select("Symbol").rdd.flatMap(lambda x: x).collect()
#print(sym_list)


## Reading the Stock data

#Schema for stock data
stock_data_schema = StructType([ \
    StructField("timestamp", StringType(), True), \
    StructField("open", DecimalType(6,2), True), \
    StructField("high", DecimalType(6,2), True), \
    StructField("low", DecimalType(6,2), True), \
    StructField("close", DecimalType(6,2), True), \
    StructField("volume", IntegerType(), True) \
])

stock_data_paths = [S3_BUCKET_PATH+x+".csv" for x in sym_list]

stock_data = spark.read.option("header", True).schema(stock_data_schema).csv(stock_data_paths)

stock_data.printSchema() #to check the Schema of the dataframe
#stock_data.show()

stock_data.withColumn("Symbol", input_file_name()).repartition(col("Symbol"))
stock_data_full = stock_data.join(sym_meta, upper(stock_data["Symbol"]) == upper(sym_meta["Symbol"]), "left")\
    .select(stock_data['*'], 
            sym_meta["Symbol"],
            sym_meta["Name"],
            sym_meta["Country"],
            sym_meta["Sector"],
            sym_meta["Industry"])

stock_data_full.printSchema()

### Summary Report (All Time)
def summary_report_all_func(stock_data_full, industries):
    summary_report_output__all_time = stock_data_full.filter(stock_data_full["Sector"].isin(industries)) \
        .groupBy(stock_data_full["Sector"])\
        .agg(
            avg(stock_data_full["open"]).alias("Avg Open Price"), \
            avg(stock_data_full["close"]).alias("Avg Close Price"), \
            max(stock_data_full["high"]).alias("Max High Price"), \
            min(stock_data_full["low"]).alias("Min Low Price"), \
            avg(stock_data_full["volume"]).alias("Avg Volume") \
        )
    
    summary_report_output__all_time.printSchema()

    #For developmental purposes I'm trying to store the data in a Dataframe and then return it; rather than directly returning it.
    return summary_report_output__all_time


no_industries = int(input("Enter the number of industries for which you wanted Summary Report (All Time):"))
arr = input()   # takes the whole line of no_industries strings
industries = list(arr.split(',')) # split those strings with ','

summary_report_all_func(stock_data_full, industries).show(n = len(sym_list))


### Summary Report (Period)
def summary_report_period_func(stock_data_full, sectors, start_date, end_date):
    summary_report_output__period = stock_data_full \
        .filter(stock_data_full["Sector"].isin(sectors), 
                to_date(stock_data_full["timestamp"], "YYYY-MM-DD").between(start_date, end_date)
        ) \
        .groupBy(stock_data_full["Sector"]) \
        .agg(
            avg(stock_data_full["open"]).alias("Avg Open Price"), \
            avg(stock_data_full["close"]).alias("Avg Close Price"), \
            max(stock_data_full["high"]).alias("Max High Price"), \
            min(stock_data_full["low"]).alias("Min Low Price"), \
            avg(stock_data_full["volume"]).alias("Avg Volume") \
        )

    #For developmental purposes I'm trying to store the data in a Dataframe and then return it; rather than directly returning it.
    return summary_report_output__period


start_date = to_date(input("Enter the start date of the period for Summary Report [YYYY-MM-DD]:"), "YYYY-MM-DD")
end_date = to_date(input("Enter the end date of the period for Summary Report [YYYY-MM-DD]:"), "YYYY-MM-DD")
no_sectors = int(input("Enter the number of sectors for which you wanted Summary Report (Given Period):"))
arr = input()   # takes the whole line of no_sectors strings
sectors = list(arr.split(',')) # split those strings with ','

summary_report_period_func(stock_data_full, sectors, start_date, end_date).show(n = len(sym_list))

### Detailed Reports (Period)
def detailed_report_period_func(stock_data_full, sectors, start_date, end_date):
    detailed_report_output__period = stock_data_full \
        .filter(stock_data_full["Sector"].isin(sectors), 
                to_date(stock_data_full["timestamp"], "YYYY-MM-DD").between(start_date, end_date)
        ) \
        .groupBy(stock_data_full["Symbol"], stock_data_full["Name"]) \
        .agg(
            avg(stock_data_full["open"]).alias("Avg Open Price"), \
            avg(stock_data_full["close"]).alias("Avg Close Price"), \
            max(stock_data_full["high"]).alias("Max High Price"), \
            min(stock_data_full["low"]).alias("Min Low Price"), \
            avg(stock_data_full["volume"]).alias("Avg Volume") \
        )

    #For developmental purposes I'm trying to store the data in a Dataframe and then return it; rather than directly returning it.
    detailed_report_output__period.printSchema()

start_date = to_date(input("Enter the start date of the period for Detailed Reports [YYYY-MM-DD]:"), "YYYY-MM-DD")
end_date = to_date(input("Enter the end date of the period for Detailed Reports [YYYY-MM-DD]:"), "YYYY-MM-DD")
no_sectors = int(input("Enter the number of sectors for which you wanted Detailed Report (Given Period):"))
arr = input()   # takes the whole line of no_sectors strings
sectors = list(arr.split(',')) # split those strings with ','

detailed_report_output__period(stock_data_full, sectors, start_date, end_date).show(n = len(sym_list))

spark.stop()