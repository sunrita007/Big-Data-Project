import sys, string
import os
import socket
import time
import io
import operator
import boto3
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import length, mean
from pyspark.sql import Row
from pyspark.sql.types import StringType, DoubleType, StructField, StructType

if __name__ == "__main__":

    # Create a Spark session
    spark = SparkSession\
        .builder\
        .appName("Ethereum")\
        .getOrCreate()

    # Set up environment variables for accessing S3 bucket
    s3_data_repository_bucket = os.environ['DATA_REPOSITORY_BUCKET']
    s3_endpoint_url = os.environ['S3_ENDPOINT_URL'] + ':' + os.environ['BUCKET_PORT']
    s3_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    s3_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    s3_bucket = os.environ['BUCKET_NAME']

    # Configure Hadoop to access S3 bucket
    hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.endpoint", s3_endpoint_url)
    hadoopConf.set("fs.s3a.access.key", s3_access_key_id)
    hadoopConf.set("fs.s3a.secret.key", s3_secret_access_key)
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")
    
    # Read blocks data from S3 bucket
    blocks = spark.read.csv("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/blocks.csv",
                           header=True, inferSchema=True)
    
    # Calculate the average length of each column in the blocks dataset
    logs_bloom_mean = blocks.select(mean(length("logs_bloom"))).collect()[0][0]
    sha3_uncles_mean = blocks.select(mean(length("sha3_uncles"))).collect()[0][0]
    transactions_root_mean = blocks.select(mean(length("transactions_root"))).collect()[0][0]
    state_root_mean = blocks.select(mean(length("state_root"))).collect()[0][0]
    receipts_root_mean = blocks.select(mean(length("receipts_root"))).collect()[0][0]

    # Calculate the average size in bytes (considering each character after the first two requires four bits)
    logs_bloom_bytes = (logs_bloom_mean - 2) * 4 / 8
    sha3_uncles_bytes = (sha3_uncles_mean - 2) * 4 / 8
    transactions_root_bytes = (transactions_root_mean - 2) * 4 / 8
    state_root_bytes = (state_root_mean - 2) * 4 / 8
    receipts_root_bytes = (receipts_root_mean - 2) * 4 / 8

    # Get the total number of rows in the dataset
    total_rows = blocks.count()

    # Calculate the total space saved by removing each column
    total_space_saved = [
        Row(column_name="logs_bloom", space_saved=logs_bloom_bytes * total_rows),
        Row(column_name="sha3_uncles", space_saved=sha3_uncles_bytes * total_rows),
        Row(column_name="transactions_root", space_saved=transactions_root_bytes * total_rows),
        Row(column_name="state_root", space_saved=state_root_bytes * total_rows),
        Row(column_name="receipts_root", space_saved=receipts_root_bytes * total_rows)
    ]
    
    # Create a schema for the results DataFrame
    schema = StructType([
        StructField("column_name", StringType(), True),
        StructField("size_bytes", DoubleType(), True)
    ])

    # Create a DataFrame with the results and the schema
    results_df = spark.createDataFrame(total_space_saved, schema)

    # Calculate the average length of each column in the blocks dataset
    all_columns = blocks.columns
    mean_lengths = blocks.select(*[mean(length(column)).alias(column) for column in all_columns]).collect()[0]

    # Calculate the average size in bytes (considering each character after the first two requires four bits)
    column_bytes = [(column, ((mean_lengths[column] - 2) * 4 / 8) if mean_lengths[column] is not None else 0.0) for column in all_columns]

    # Get the total number of rows in the dataset
    total_rows = blocks.count()

    # Calculate the total space saved by removing each column
    total_space_saved = [
        Row(column_name=column, space_saved=bytes * total_rows)
        for column, bytes in column_bytes
    ]

    # Calculate the total space occupied by the dataset
    total_space_occupied = sum([row.space_saved for row in total_space_saved])

    # Calculate the percentage of total space saved by removing each column
    percentage_space_saved = [
        Row(column_name=row.column_name,
            space_saved=row.space_saved,
            percentage_saved=(row.space_saved / total_space_occupied) * 100)
        for row in total_space_saved
    ]

    # Create a schema for the percentage space saved DataFrame
    percentage_schema = StructType([
        StructField("column_name", StringType(), True),
        StructField("space_saved", DoubleType(), True),
        StructField("percentage_saved", DoubleType(), True)
    ])

    # Create a DataFrame with the percentage space saved results and the schema
    percentage_results_df = spark.createDataFrame(percentage_space_saved, percentage_schema)

    # Get the current date and time for the output filename
    now = datetime.now()
    date_time = now.strftime("%d-%m-%Y_%H_%M_%S")

    # Construct the output file name with the date and time
    output_file_name1 = f"overhead_{date_time}.txt"
    output_file_name2 = f"percentage_saved_{date_time}.txt"

    # Convert the results DataFrame to a CSV string
    csv_buffer = io.StringIO()
    results_df.toPandas().to_csv(csv_buffer, index=False, header=True, sep=',', line_terminator='\n')
    csv_buffer_percentage = io.StringIO()
    percentage_results_df.toPandas().to_csv(csv_buffer_percentage, index=False, header=True, sep=',', line_terminator='\n')
    
    # Set up S3 bucket resource for storing the results
    my_bucket_resource = boto3.resource('s3',
                                        endpoint_url='http://' + s3_endpoint_url,
                                        aws_access_key_id=s3_access_key_id,
                                        aws_secret_access_key=s3_secret_access_key)

    #write output to files in s3 bucket
    my_result_object = my_bucket_resource.Object(s3_bucket,output_file_name1)
    my_result_object.put(Body=csv_buffer.getvalue())
    my_result_object = my_bucket_resource.Object(s3_bucket,output_file_name2)
    my_result_object.put(Body=csv_buffer_percentage.getvalue())

    # Stop the Spark session
    spark.stop()