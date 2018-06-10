from __future__ import print_function
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext

# Bunch of imports (may need more)
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder ,CrossValidatorModel
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import CountVectorizer

from pyspark.sql.types import ArrayType, IntegerType, StringType
from pyspark.sql.functions import col, split

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
sc.setLogLevel("WARN")
# sc = SparkContext.getOrCreate()
# sqlContext = SQLContext(sc)
# sc.addPyFile("cleantext.py")
# TODO change paths when submitting

# TASK 1
# Code for task 1...

comments = sqlContext.read.json("/home/cs143/data/comments-minimal.json.bz2") #gives the attibutes and its type
# comments = comments.sample(False, 0.02, None) #TODO this only use 20% of data. Remove this when submitting!!
submissions = sqlContext.read.json("/home/cs143/data/submissions.json.bz2") #gives the attibutes and its type
labeled_data = sqlContext.read.csv("labeled_data.csv", header=True, mode="DROPMALFORMED")

# make parquet

comments.write.parquet("comments") #TODO if you want to keep the 100% data (now we have 2%), rename parquet files to something else
labeled_data.write.parquet("labeled_data") #TODO if you want to keep the 100% data (now we have 2%), rename parquet files to something else
submissions.write.parquet("submissions") #TODO if you want to keep the 100% data (now we have 2%), rename parquet files to something else

comments = sqlContext.read.parquet("comments")
submissions = sqlContext.read.parquet("submissions")
labeled_data = sqlContext.read.parquet("labeled_data")

states = ['Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California', 'Colorado',
    'Connecticut', 'Delaware', 'District of Columbia', 'Florida', 'Georgia',
    'Hawaii', 'Idaho', 'Illinois', 'Indiana', 'Iowa', 'Kansas', 'Kentucky',
    'Louisiana', 'Maine', 'Maryland', 'Massachusetts', 'Michigan', 'Minnesota',
    'Mississippi', 'Missouri', 'Montana', 'Nebraska', 'Nevada', 'New Hampshire',
    'New Jersey', 'New Mexico', 'New York', 'North Carolina', 'North Dakota',
    'Ohio', 'Oklahoma', 'Oregon', 'Pennsylvania', 'Rhode Island',
    'South Carolina', 'South Dakota', 'Tennessee', 'Texas', 'Utah', 'Vermont',
    'Virginia', 'Washington', 'West Virginia', 'Wisconsin', 'Wyoming']

# TASK 1
# Code for task 1...
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

# TASK 2
# Code for task 2...

# functional dependencies implied by the data.
def task2():
    comments.createOrReplaceTempView("comment_table")
    #comment_table = spark.sql("SELECT id, body FROM cmnt_table")

    labeled_data.createOrReplaceTempView("data_table")

    # csv_table = spark.sql("SELECT * FROM df_table")

    query = spark.sql("SELECT data_table.Input_id, data_table.labeldem, data_table.labelgop, data_table.labeldjt, comment_table.body as comment_body FROM data_table JOIN comment_table ON data_table.Input_id = comment_table.id")
    query.write.saveAsTable("task2_table")

# TASK 4, 5
# Code for tasks 4 and 5

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
    #print(arr, file=open("output1.txt", "a"))
    return ', '.join(arr) #I changed this back to a string of a, b, c, d, e, etc..... Because the form [a,b,c,d,e] is STILL A STRING, not an array.

# TASK 4, 5
# Code for tasks 4 and 5

def task4():
    spark.udf.register("sanitize", cleantext.sanitize)  # UDF
    spark.udf.register("connect_all_string", connect_all_string)


    querytask4 = spark.sql("SELECT Input_id, connect_all_string(sanitize(comment_body)) AS n_grams, labeldjt  FROM task2_table")
    querytask4.write.saveAsTable("task4_table")

    #querytask4.show()

# TASK 6A, 6B
# Code for tasks 6A and 6B

def task6():

    querytask6 = spark.sql("SELECT Input_id,n_grams , IF(labeldjt='1',1,0) AS positive_djt, IF(labeldjt='-1',1,0) AS negative_djt FROM task4_table")
    #querytask6.show()
    #reference https://stackoverflow.com/questions/38189088/convert-comma-separated-string-to-array-in-pyspark-dataframe
    querytask6= querytask6.select(split(col("n_grams"), ",\s*").alias("n_grams"),col("positive_djt"),col("negative_djt")) #convert the "combined n_grams " from string form to actual array form

    #reference: http://spark.apache.org/docs/2.2.0/api/python/pyspark.ml.html
    cv = CountVectorizer(minDF=5.0, vocabSize=1 << 18, binary=True, inputCol="n_grams", outputCol="features")
    model = cv.fit(querytask6)
    task6Result = model.transform(querytask6)
    task6Result.printSchema() # for a better look of the table , remove "truncate=False"
    task6Result.write.saveAsTable("task6_table")
    return model

# TASK 7
# Code for task 7

def modelfit():
    # Initialize two logistic regression models.
    # Replace labelCol with the column containing the label, and featuresCol with the column containing the features.
    pos = spark.sql("SELECT features,positive_djt  AS label FROM task6_table ")
    neg = spark.sql("SELECT features ,negative_djt  AS label FROM task6_table ")
    poslr = LogisticRegression(
        labelCol="label", featuresCol="features", maxIter=10).setThreshold(0.2)
    neglr = LogisticRegression(
        labelCol="label", featuresCol="features", maxIter=10).setThreshold(0.25)
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
    posModel.write().overwrite().save("www/pos.model")
    negModel.write().overwrite().save("www/neg.model")
    #return posModel,negModel


# TASK 8
# Code for task 8

def task8():
    #1
    comments.createOrReplaceTempView("comment_data")
    submissions.createOrReplaceTempView("submission_data")
    #sqlDF = spark.sql("SELECT comment_data.created_utc as comment_timestamp, comment_data.id, comment_data.body FROM comment_data JOIN (SELECT title FROM comment_data JOIN submission_data ON (Replace(comment_data.link_id, 't3_', '')) = submission_data.id) t2 ON ")
    sqlDF = spark.sql("SELECT comment_data.id, comment_data.created_utc as comment_timestamp, comment_data.body AS comment_body, submission_data.title, submission_data.author_flair_text as state FROM comment_data JOIN submission_data ON (Replace(comment_data.link_id, 't3_', '')) = submission_data.id")
    # sqlDF.show() #debugging purpose
    sqlDF.write.saveAsTable("task8_table")

# TASK 9
# Code for task 9

def task9(task6model):
    querytask9_0 = spark.sql("SELECT id,comment_timestamp,title,state,comment_body FROM task8_table  WHERE comment_body NOT LIKE '&gt%' AND comment_body NOT LIKE '%/s%'")
    querytask9_0.write.saveAsTable("task9_table1")
    querytask9_1 = spark.sql("SELECT id, connect_all_string(sanitize(comment_body)) AS n_grams, comment_timestamp,title,state,comment_body  FROM task9_table1")
    querytask9_2= querytask9_1.select(split(col("n_grams"), ",\s*").alias("n_grams") ,col("id") ,col("comment_timestamp"),col("title"),col("state"),col("comment_body"))

    task9df = task6model.transform(querytask9_2)
    task9df.printSchema()
    task9df = task9df.write.saveAsTable("task9_table2")
    querytask9_3 = spark.sql("SELECT id,  n_grams, comment_timestamp,title,state,comment_body, features, features AS features_backup  FROM task9_table2")

    model_pos = CrossValidatorModel.load("www/pos.model")
    model_neg = CrossValidatorModel.load("www/neg.model")
    pos_ans = model_pos.transform(querytask9_3).write.saveAsTable("pos_table")

    task9df_withPos = spark.sql("SELECT id,comment_timestamp,title,state,comment_body,prediction AS pos, features_backup AS features, probability AS pos_probability  FROM pos_table")
    task9df_withPos.show()
    neg_ans = model_neg.transform(task9df_withPos).write.saveAsTable("neg_table")

    task9result = spark.sql("SELECT id,comment_timestamp,title,state,comment_body, pos , prediction AS neg FROM neg_table")

    task9result.write.parquet("task9result_parquet") #store parquet


    final_task9result = spark.read.parquet("task9result_parquet")
    final_task9result.write.saveAsTable("task9_table")
    spark.sql("SELECT * FROM task9_table").show()

# TASK 10
# Code for task 10
def task10():
    df = spark.createDataFrame(states, StringType()).write.saveAsTable("states_table")
    comments.createOrReplaceTempView("comment_data")
    submissions.createOrReplaceTempView("submission_data")
    # across all posts
    all_posts = spark.sql("SELECT SUM(CASE WHEN pos = 1 THEN 1 ELSE 0 END) / COUNT(*) as pos_perc, SUM(CASE WHEN neg = 1 THEN 1 ELSE 0 END) / COUNT(*) as neg_perc FROM task9_table")
    # all_posts.show()
    all_posts.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("all_posts.csv")


    # across all days 
    all_days = spark.sql("SELECT DATE(FROM_UNIXTIME(comment_timestamp)) as `date`, SUM(CASE WHEN pos = 1 THEN 1 ELSE 0 END) / COUNT(*) as Positive, SUM(CASE WHEN neg = 1 THEN 1 ELSE 0 END) / COUNT(*) as Negative FROM task9_table GROUP BY DATE(FROM_UNIXTIME(comment_timestamp))")
    # all_days.show()
    all_days.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("all_days.csv")

    # across all states
    query_string = "SELECT task9_table.state as state, SUM(CASE WHEN pos = 1 THEN 1 ELSE 0 END) / COUNT(*) as Positive, SUM(CASE WHEN neg = 1 THEN 1 ELSE 0 END) / COUNT(*) as Negative FROM task9_table JOIN states_table ON task9_table.state = states_table.value GROUP BY state"
    all_states = spark.sql(query_string)
    # all_states.show()
    all_states.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("all_states.csv")

    # across comment score
    all_comment_score = spark.sql("SELECT comment_data.score as comment_score, SUM(CASE WHEN pos = 1 THEN 1 ELSE 0 END) / COUNT(*) as Positive, SUM(CASE WHEN neg = 1 THEN 1 ELSE 0 END) / COUNT(*) as Negative FROM task9_table JOIN comment_data ON task9_table.id = comment_data.id GROUP BY comment_data.score")
    # all_comment_score.show()
    all_comment_score.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("all_comment_score.csv")

    # across story score (submission score)
    all_submission_score = spark.sql("SELECT submission_data.score as submission_score, SUM(CASE WHEN pos = 1 THEN 1 ELSE 0 END) / COUNT(*) as Positive, SUM(CASE WHEN neg = 1 THEN 1 ELSE 0 END) / COUNT(*) as Negative FROM task9_table JOIN comment_data ON task9_table.id = comment_data.id JOIN submission_data ON (Replace(comment_data.link_id, 't3_', '')) = submission_data.id GROUP BY submission_data.score")
    # all_submission_score.show()
    all_submission_score.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("all_submission_score.csv")

    # test = "SELECT submission_data.id as ID, SUM(CASE WHEN pos = 1 THEN 1 ELSE 0 END) / COUNT(*) as Positive FROM comment_data JOIN task9_table ON comment_data.id = task9_table.id JOIN submission_data ON (Replace(comment_data.link_id, 't3_', '')) = submission_data.id GROUP BY submission_data.id ORDER BY Positive DESC LIMIT 10"
    # spark.sql(test).show()

def main(context):
    """Main function takes a Spark SQL context."""
    # YOUR CODE HERE
    # YOU MAY ADD OTHER FUNCTIONS AS NEEDED
    task2()
    task4() #and 5
    task6model = task6()
    modelfit() #task 7
    task8()
    task9(task6model)
    task10()


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

