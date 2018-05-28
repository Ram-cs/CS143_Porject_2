from __future__ import print_function
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext

# IMPORT OTHER MODULES HERE
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
import pyspark as spark
import itertools
from itertools import chain
from pyspark.sql.types import *

from pyspark.sql import Row


#Loading a BZ2 file containing JSON objects into Spark:
# sc = SparkContext.getOrCreate()
# sqlContext = SQLContext(sc)
# sc.addPyFile("cleantext.py")
# comments = sqlContext.read.json("comments-minimal.json.bz2") #gives the attibutes and its type
# submissions = sqlContext.read.json("submissions.json.bz2") #gives the attibutes and its type

#TASK 1
#run spark frame
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()



def comment_laveledData():
    df_cmnt = spark.read.json("comments.json")
    df_cmnt.show() #show schema
    df_cmnt.printSchema() #show attibutes with its type

def submission_levelData():
    df_sb = spark.read.json("submission.json")
    df_sb.show()#show schema
    df_sb.printSchema() #show attibutes with its type

def read_csv_file():
    df_csv = spark.read.csv('labeled_data.csv')
    df_csv.printSchema()
    df_csv.describe().show()#give summary that include count, mean, stddev, min, max

#TASK 2
#functional dependencies implied by the data.



def main(context):
    """Main function takes a Spark SQL context."""
    # YOUR CODE HERE
    # YOU MAY ADD OTHER FUNCTIONS AS NEEDED

if __name__ == "__main__":
    comment_laveledData()
    submission_levelData()
    read_csv_file()

    # conf = SparkConf().setAppName("CS143 Project 2B")
    # conf = conf.setMaster("local[*]")
    # sc   = SparkContext(conf=conf)
    # sqlContext = SQLContext(sc)
    # sc.addPyFile("cleantext.py")
    # main(sqlContext)

