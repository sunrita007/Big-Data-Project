import sys, string
import os
import socket
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import month, year
from datetime import datetime

# ccc create spark timeanalysis.py -s -e 10 -c 4
# ccc method bucket cp -r bkt:avgtxn.csv/ avgtxn
# ccc method bucket cp -r bkt:numtxn.csv/ numtxn 
if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("ETHTxnCount")\
        .getOrCreate()
    #spark.conf.set("spark.executor.instances", 3)
    #spark.conf.set("spark.executor.cores", 4)
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
    
    
    df = spark.read.csv("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/transactions.csv", header=True)
    df2 = df
    #.limit(10000)
    #df2 = df2.withColumn('date', F.to_date('timestamp', 'yyyy-MM-dd'))
    df2 = df2.withColumn('date', F.from_unixtime('block_timestamp', 'yyyy-MM-dd'))
    num_txn = df2.select("block_number", month("date").alias("month"), year("date").alias("year"))
    num_txn_grp = num_txn.groupby("year","month").count().orderBy("year","month")
    avg_txn = df2.select(df2["value"].cast("int").alias("value"), month("date").alias("month"), year("date").alias("year"))
    avg_txn_grp = avg_txn.groupby("year","month").avg("value").orderBy("year","month")
    print("Number of transactions.......\n\n\n")
    num_txn_grp.show(10)
    print("Average value of transactions.......\n\n\n")
    avg_txn_grp.show(10)
    print("df shown.....now generating CSV\n\n\n")
    #num_txn_grp.limit(10).write.mode("overwrite").csv("s3a://" + s3_bucket + '/output.csv', header=True)
    num_txn_grp.coalesce(1).write.mode("overwrite").csv("s3a://" + s3_bucket + '/numtxn.csv', header=True)
    avg_txn_grp.coalesce(1).write.mode("overwrite").csv("s3a://" + s3_bucket + '/avgtxn.csv', header=True)
    print("DONE!!")
    spark.stop()
