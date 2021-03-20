import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkConf
import csv
from io import StringIO
import logging
import os

# Setting up logger format
logging.basicConfig(format='%(asctime)s\t%(levelname)s\t%(message)s', datefmt='%d-%b-%y %H:%M:%S')
logger = logging.getLogger('price-paid-to-json')
logger.setLevel(logging.DEBUG)

# Download Artifacts
conf = SparkConf()
conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.2.0')
conf.set('spark.hadoop.fs.s3a.access.key', 'foobar')
conf.set('spark.hadoop.fs.s3a.secret.key', 'foobar')
conf.set('spark.hadoop.fs.s3a.endpoint', 'localhost:4566')
conf.set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
conf.set("spark.hadoop.fs.s3a.path.style.access", "true")

logger.info("Getting Spark Session and Context")
spark = SparkSession.builder.config("spark.driver.extraClassPath", "postgresql-42.2.19.jar") \
					.config(conf=conf) \
					.master("local[1]") \
                    .appName('test') \
                    .getOrCreate()

sc = spark.sparkContext

def readLocal():
	# Reading CSV files from local drive, removed for Git
	# Uses data from https://data.police.uk/data/
	logger.info("Reading local files")
	global raw_content 
	raw_content = sc.textFile("data/*/*.csv")

	# Getting Count
	logger.info("Count of raw data")
	print(raw_content.count())

def readS3():
	global raw_content 
	raw_content = sc.textFile('s3a://crime-data/*/*.csv')

	# Getting Count
	logger.info("Count of raw data")
	print(raw_content.count())

def convertToCsv(x):

	data = StringIO(x)
	reader = csv.reader(data, delimiter=',')
	for row in reader: 
		return row

def getCountsViaReduceByKey():
	# Converting to CSV and remove headers
	logger.info("Converting to CSV and removing headers")
	content = raw_content.map(convertToCsv).filter(lambda x: x[1] != 'Month')

	# Print top 5 results
	logger.info("Printing top 5 results")
	print(content.take(5))

	# Creates a new object for counting by extracting month and 1
	logger.info("New object with month and 1 for counting")
	month_count = content.map(lambda x: (x[1], 1))

	# Do the count
	logger.info("Doing the count for the months")
	month_count = month_count.reduceByKey(lambda a,b: a+b).filter(lambda x: x[1] != 'Month')

	logger.info("Print of 5 months")
	print(month_count.take(5))

def getCountsViaDF():
	# Cleanly convert data into CSV
	logger.info("Converting to CSV")
	content = raw_content.map(convertToCsv)

	logger.info("Printing out sample")
	print(content.take(10))

	# Creates Data Frame, the data is all rows where the second value is not a header, schema is the opposite
	logger.info("Creating data frame")
	df = spark.createDataFrame(data = content.filter(lambda x:x[1]!='Month'), schema=content.filter(lambda x:x[1]=='Month').collect()[0])

	logger.info("Group by Month")
	month_df = df.groupBy("Month").count()
	month_df.show()

	logger.info("Writing months count to disk")
	month_df.write.csv('months')

	logger.info("Group by Crime Type")
	crime_df = df.groupBy("Crime type").count()
	crime_df.show()

	logger.info("Writing crimes count to disk")
	crime_df.write.csv('crimes')

def importToPG():
	# Converting to CSV and remove headers
	logger.info("Converting to CSV and removing headers")
	content = raw_content.map(convertToCsv)

	# Creates Data Frame, the data is all rows where the second value is not a header, schema is the opposite
	logger.info("Creating data frame")
	df = spark.createDataFrame(data = content.filter(lambda x:x[1]!='Month'), schema=content.filter(lambda x:x[1]=='Month').collect()[0])

	df.write.format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/postgres") \
    .option("dbtable", "public.crimes") \
    .option("user", "postgres") \
    .option("password", "mysecretpassword") \
    .mode("overwrite") \
    .save()

	
logger.info("Spark Sandbox Demo")

logger.info("Getting Data")
#readLocal()
readS3()

#logger.info("Running counts via Reduce By Key")
#getCountsViaReduceByKey()

logger.info("Running counts via Data Frame")
getCountsViaDF()

#logger.info("Import into Postgres")
#importToPG()

sc.stop()