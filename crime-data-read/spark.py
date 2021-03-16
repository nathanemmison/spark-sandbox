import pyspark
from pyspark.sql import SparkSession
import csv
from io import StringIO
import logging

# Setting up logger format
logging.basicConfig(format='%(asctime)s\t%(levelname)s\t%(message)s', datefmt='%d-%b-%y %H:%M:%S')
logger = logging.getLogger('price-paid-to-json')
logger.setLevel(logging.DEBUG)

logger.info("Getting Spark Session and Context")
spark = SparkSession.builder.master("local[1]") \
                    .appName('test') \
                    .getOrCreate()

sc = spark.sparkContext

# Reading CSV files from local drive, removed for Git
# Uses data from https://data.police.uk/data/
logger.info("Reading local files")
raw_content = sc.textFile("data/*/*.csv")

# Getting Count
logger.info("Count of raw data")
print(raw_content.count())

# Custom Function to generate a clean CSV line
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
	
logger.info("Spark Sandbox Demo")

logger.info("Running counts via Reduce By Key")
getCountsViaReduceByKey()

logger.info("Running counts via Data Frame")
getCountsViaDF()

sc.stop()