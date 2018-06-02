from __future__ import print_function
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext

# Bunch of imports (may need more)
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import BinaryClassificationEvaluator

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

# Loading a BZ2 file containing JSON objects into Spark:
conf = SparkConf().setAppName("CS143 Project 2B")
conf = conf.setMaster("local[*]")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
sc.addPyFile("cleantext.py")
# sc = SparkContext.getOrCreate()
# sqlContext = SQLContext(sc)
# sc.addPyFile("cleantext.py")
# TODO change paths when submitting
# comments = sqlContext.read.json("/home/cs143/data/comments-minimal.json.bz2") #gives the attibutes and its type
# submissions = sqlContext.read.json("/home/cs143/data/submissions.json.bz2") #gives the attibutes and its type
# labeled_data = sqlContext.read.csv("labeled_data.csv", header=True, mode="DROPMALFORMED")

# make parquet
# comments.write.parquet("comments")
# labeled_data.write.parquet("labeled_data")
# submissions.write.parquet("submissions")

comments = sqlContext.read.parquet("comments")
submissions = sqlContext.read.parquet("submissions")
labeled_data = sqlContext.read.parquet("labeled_data")

# TASK 1
# run spark frame
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()


# def comment_laveledData():
#     df_cmnt = spark.read.json("comments.json")
#     df_cmnt.show() #show schema
#     df_cmnt.printSchema() #show attibutes with its type

# def submission_levelData():
#     df_sb = spark.read.json("submission.json")
#     df_sb.show()#show schema
#     df_sb.printSchema() #show attibutes with its type

# def read_csv_file():
#     df_csv = spark.read.csv('labeled_data.csv')
#     df_csv.printSchema()
#     df_csv.describe().show()#give summary that include count, mean, stddev, min, max

# TASK 2
# functional dependencies implied by the data.

def task2():
    comments.createOrReplaceTempView("comment_table")
    #comment_table = spark.sql("SELECT id, body FROM cmnt_table")

    labeled_data.createOrReplaceTempView("data_table")

    # csv_table = spark.sql("SELECT * FROM df_table")

    query = spark.sql("SELECT data_table.Input_id, data_table.labeldem, data_table.labelgop, data_table.labeldjt, comment_table.body as comment_body FROM data_table JOIN comment_table ON data_table.Input_id = comment_table.id")
    # query.show()
    query.write.saveAsTable("task2_table")


def connect_all_string(string_list):
    arr = []
    # unigram
    for gram in string_list[1].split():
        arr.append(gram)
    # bigram
    for gram in string_list[2].split():
        arr.append(gram)
    # trigram
    for gram in string_list[3].split():
        arr.append(gram)
    print(arr, file=open("output1.txt", "a"))
    return arr

# maybe task5 too?


def task4():
    spark.udf.register("sanitize", cleantext.sanitize)  # UDF
    spark.udf.register("connect_all_string", connect_all_string, ArrayType(StringType()))

    query = spark.sql(
        "SELECT Input_id, connect_all_string(sanitize(comment_body)) AS n_grams  FROM task2_table")
    # query.show()
    # query.printSchema()
    query.write.saveAsTable("task4_table")

#task 6A and 6B
def task6():

    querytask6 = spark.sql("SELECT Input_id,n_grams, IF(labeldjt='1','1','0') AS positive_djt, IF(labeldjt='-1','1','0') AS negative_djt FROM task4_table")
    querytask6.show()
    #reference https://stackoverflow.com/questions/38189088/convert-comma-separated-string-to-array-in-pyspark-dataframe
    # querytask6= querytask6.select(split(col("n_grams"), ",\s*").alias("n_grams"),col("positive_djt"),col("negative_djt")) #convert the "combined n_grams " from string form to actual array form

    # #reference: http://spark.apache.org/docs/2.2.0/api/python/pyspark.ml.html
    # cv = CountVectorizer(minDF=5.0, vocabSize=1 << 18, binary=True, inputCol="n_grams", outputCol="vectors")
    # model = cv.fit(querytask6)
    # task6Result = model.transform(querytask6)
    # task6Result.show(truncate=False) # for a better look of the table , remove "truncate=False"


def modelfit():
    # Initialize two logistic regression models.
    # Replace labelCol with the column containing the label, and featuresCol with the column containing the features.
    poslr = LogisticRegression(
        labelCol="poslabel", featuresCol="features", maxIter=10)
    neglr = LogisticRegression(
        labelCol="neglabel", featuresCol="features", maxIter=10)
    # This is a binary classifier so we need an evaluator that knows how to deal with binary classifiers.
    posEvaluator = BinaryClassificationEvaluator()
    negEvaluator = BinaryClassificationEvaluator()
    # There are a few parameters associated with logistic regression. We do not know what they are a priori.
    # We do a grid search to find the best parameters. We can replace [1.0] with a list of values to try.
    # We will assume the parameter is 0.3. Grid search takes forever.
    posParamGrid = ParamGridBuilder().addGrid(poslr.regParam, [1.0]).build()
    negParamGrid = ParamGridBuilder().addGrid(neglr.regParam, [1.0]).build()
    # We initialize a 5 fold cross-validation pipeline.
    posCrossval = CrossValidator(
        estimator=poslr,
        evaluator=posEvaluator,
        estimatorParamMaps=posParamGrid,
        numFolds=5)
    negCrossval = CrossValidator(
        estimator=neglr,
        evaluator=negEvaluator,
        estimatorParamMaps=negParamGrid,
        numFolds=5)
    # Although crossvalidation creates its own train/test sets for
    # tuning, we still need a labeled test set, because it is not
    # accessible from the crossvalidator (argh!)
    # Split the data 50/50
    posTrain, posTest = pos.randomSplit([0.5, 0.5])
    negTrain, negTest = neg.randomSplit([0.5, 0.5])
    # Train the models
    print("Training positive classifier...")
    posModel = posCrossval.fit(posTrain)
    print("Training negative classifier...")
    negModel = negCrossval.fit(negTrain)

    # Once we train the models, we don't want to do it again. We can save the models and load them again later.
    posModel.save("www/pos.model")
    negModel.save("www/neg.model")


def main(context):
    """Main function takes a Spark SQL context."""
    # YOUR CODE HERE
    # YOU MAY ADD OTHER FUNCTIONS AS NEEDED
    task2()
    task4()
    task6()

if __name__ == "__main__":
    # comment_laveledData()
    # submission_levelData()
    # read_csv_file()

    # conf = SparkConf().setAppName("CS143 Project 2B")
    # conf = conf.setMaster("local[*]")
    # sc   = SparkContext(conf=conf)
    # sqlContext = SQLContext(sc)
    # sc.addPyFile("cleantext.py")
    main(sqlContext)
