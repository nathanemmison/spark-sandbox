import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkConf
import csv
from io import StringIO
import logging
import os

# Setting up logger format
logging.basicConfig(format='%(asctime)s\t%(levelname)s\t%(message)s', datefmt='%d-%b-%y %H:%M:%S')
logger = logging.getLogger('imdb-data')
logger.setLevel(logging.DEBUG)

# Download Artifacts
conf = SparkConf()
conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.2.0')

logger.info("Getting Spark Session and Context")
spark = SparkSession.builder.config("spark.driver.extraClassPath", "postgresql-42.2.19.jar") \
    .config(conf=conf) \
    .master("local[1]") \
    .appName('test') \
    .getOrCreate()

sc = spark.sparkContext


def read_local():
    # Reading CSV files from local drive, removed for Git
    # Uses data from https://data.police.uk/data/
    logger.info("Reading local files")
    global raw_content
    raw_content = sc.textFile("data/titles.tsv")


# Getting Count
# logger.info("Count of raw data")
# print(raw_content.count())

def convert_to_tsv(x):
    x = x.replace("\\N", "")
    data = StringIO(x)
    reader = csv.reader(data, delimiter='\t', quoting=csv.QUOTE_NONE)
    for row in reader:

        if len(row) != 9:
            print(row)
        else:
            return row


def import_to_pg():
    # Converting to CSV and remove headers
    logger.info("Converting to CSV and removing headers")
    content = raw_content.filter(lambda x: len(x.split("\t")) == 9).map(convert_to_tsv())

    # Creates Data Frame, the data is all rows where the second value is not a header, schema is the opposite
    logger.info("Creating data frame")
    df = spark.createDataFrame(data=content.filter(lambda x: x[0] != 'tconst'),
                               schema=content.filter(lambda x: x[0] == 'tconst').collect()[0])

    logger.info("Writing Films to database")
    df.filter(df.titleType == "movie").write.format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/postgres") \
        .option("dbtable", "public.films") \
        .option("user", "postgres") \
        .option("password", "mysecretpassword") \
        .mode("overwrite") \
        .save()

    logger.info("Writing TV Series' to database")
    df.filter(df.titleType == "tvSeries").write.format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/postgres") \
        .option("dbtable", "public.tv_series") \
        .option("user", "postgres") \
        .option("password", "mysecretpassword") \
        .mode("overwrite") \
        .save()


logger.info("Spark Sandbox Demo")

logger.info("Getting Data")
read_local()

logger.info("Import into Postgres")
import_to_pg()

sc.stop()
