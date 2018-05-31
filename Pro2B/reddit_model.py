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
import sys
from pyspark.sql import Row

import cleantext


#Loading a BZ2 file containing JSON objects into Spark:
# sc = SparkContext.getOrCreate()
# sqlContext = SQLContext(sc)
# sc.addPyFile("cleantext.py")
# comments = sqlContext.read.json("/home/cs143/data/comments-minimal.json.bz2") #gives the attibutes and its type
# submissions = sqlContext.read.json("/home/cs143/data/submissions.json.bz2") #gives the attibutes and its type
# labeled_data = sqlContext.read.csv("labeled_data.csv", header=True, mode="DROPMALFORMED")

#TASK 1
#run spark frame

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()



def comment_laveledData():
    df_cmnt = spark.read.parquet("comments")
    #df_cmnt = spark.read.json("comments-minimal.json")
    #df_cmnt.show() #show schema
    #df_cmnt.printSchema() #show attibutes with its type
    return df_cmnt

def submission_levelData():
    df_sb = spark.read.parquet("submissions")
    #df_sb = spark.read.json("submission.json")
    #df_sb.show()#show schema
    #df_sb.printSchema() #show attibutes with its type
    return df_sb

def read_csv_file():
    df_csv = spark.read.parquet("labeled_data")
    #df_csv = spark.read.csv('labeled_data.csv',header=True)
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

    spark.sql("DROP TABLE IF EXISTS task2_table")
    spark.sql("SELECT df_table.Input_id AS Input_id, df_table.labeldem, df_table.labelgop, df_table.labeldjt, cmnt_table.body AS comment_body FROM df_table JOIN cmnt_table ON df_table.Input_id = cmnt_table.id LIMIT 5").write.saveAsTable("task2_table")


def connect_all_string(string_list):
    str = ' '.join(string_list)
    print(str, file=open("output1.txt", "w"))
    return str

#maybe task5 too?
def task4():
    spark.udf.register("sanitize", cleantext.sanitize); #UDF
    spark.udf.register("connect_all_string", connect_all_string);
    spark.sql("SELECT Input_id, connect_all_string(sanitize(comment_body)) AS n_grams  FROM task2_table").show()

def main(context):
    """Main function takes a Spark SQL context."""
    # YOUR CODE HERE
    # YOU MAY ADD OTHER FUNCTIONS AS NEEDED

if __name__ == "__main__":

#conf = SparkConf().setAppName("CS143 Project 2B")
#conf = conf.setMaster("local[*]")
#sc   = SparkContext(conf=conf)
#sqlContext = SQLContext(sc)
#sc.addPyFile("cleantext.py")
#main(sqlContext)
    # spark.udf.register("sanitize", cleantext.sanitize); #UDF
    # spark.udf.register("connect_all_string", connect_all_string);
    df_cmnt = comment_laveledData()
    submission_levelData()
    df_csv = read_csv_file()
    task2(df_csv,df_cmnt)
    task4()




