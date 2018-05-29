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
    #df_cmnt.show() #show schema
    #df_cmnt.printSchema() #show attibutes with its type
    return df_cmnt

def submission_levelData():
    df_sb = spark.read.json("submission.json")
    #df_sb.show()#show schema
    #df_sb.printSchema() #show attibutes with its type
    return df_sb

def read_csv_file():
    df_csv = spark.read.csv('labeled_data.csv',header=True)
    #df_csv.printSchema()
    #df_csv.describe().show()#give summary that include count, mean, stddev, min, max
    return df_csv

#TASK 2
#functional dependencies implied by the data.

def task2(df_csv,df_cmnt):

    
    df_cmnt.createOrReplaceTempView("cmnt_table")
    #comment_table = spark.sql("SELECT id, body FROM cmnt_table")
    
    df_csv.createOrReplaceTempView("df_table")
    
    # csv_table = spark.sql("SELECT * FROM df_table")

    
    spark.sql("SELECT df_table.Input_id, df_table.labeldem, df_table.labelgop, df_table.labeldjt, cmnt_table.body FROM df_table JOIN cmnt_table ON df_table.Input_id = cmnt_table.id").show()

#csv_table.show()
#   comment_table.show()

def main(context):
    """Main function takes a Spark SQL context."""
    # YOUR CODE HERE
    # YOU MAY ADD OTHER FUNCTIONS AS NEEDED

if __name__ == "__main__":
    df_cmnt = comment_laveledData()
    submission_levelData()
    df_csv = read_csv_file()
    task2(df_csv,df_cmnt)

    # conf = SparkConf().setAppName("CS143 Project 2B")
    # conf = conf.setMaster("local[*]")
    # sc   = SparkContext(conf=conf)
    # sqlContext = SQLContext(sc)
    # sc.addPyFile("cleantext.py")
    # main(sqlContext)


