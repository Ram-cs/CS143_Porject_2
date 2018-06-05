from __future__ import print_function
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext

# Bunch of imports (may need more)
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import CountVectorizer

from pyspark.sql.types import ArrayType, IntegerType
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
# sc = SparkContext.getOrCreate()
# sqlContext = SQLContext(sc)
# sc.addPyFile("cleantext.py")
# TODO change paths when submitting
#comments = sqlContext.read.json("/home/cs143/data/comments-minimal.json.bz2") #gives the attibutes and its type
#submissions = sqlContext.read.json("/home/cs143/data/submissions.json.bz2") #gives the attibutes and its type
#labeled_data = sqlContext.read.csv("labeled_data.csv", header=True, mode="DROPMALFORMED")

# make parquet
#comments.write.parquet("comments")
#labeled_data.write.parquet("labeled_data")
#submissions.write.parquet("submissions")
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
    #print(arr, file=open("output1.txt", "a"))
    return ', '.join(arr) #I changed this back to a string of a, b, c, d, e, etc..... Because the form [a,b,c,d,e] is STILL A STRING, not an array.

# maybe task5 too?


def task4():
    spark.udf.register("sanitize", cleantext.sanitize)  # UDF
    spark.udf.register("connect_all_string", connect_all_string)


    querytask4 = spark.sql("SELECT Input_id, connect_all_string(sanitize(comment_body)) AS n_grams, labeldjt  FROM task2_table")
    querytask4.write.saveAsTable("task4_table")

    #querytask4.show()

#task 6A and 6B
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
    task6Result.write.saveAsTable("task6_table");

#task 7
def modelfit():
    # Initialize two logistic regression models.
    # Replace labelCol with the column containing the label, and featuresCol with the column containing the features.
    pos = spark.sql("SELECT features,positive_djt  AS label FROM task6_table ")
    neg = spark.sql("SELECT features ,negative_djt  AS label FROM task6_table ")
    poslr = LogisticRegression(
        labelCol="label", featuresCol="features", maxIter=10)
    neglr = LogisticRegression(
        labelCol="label", featuresCol="features", maxIter=10)
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

#task 8
def task8():
    #1
    comments.createOrReplaceTempView("comment_data")
    sqlDF = spark.sql("SELECT created_utc as comment_timestamp FROM comment_data")
    sqlDF.show() #debugging purpose
    #sqlDF.write.saveAsTable("task8_timestamp")

    #2
    submissions.createOrReplaceTempView("submission_data")
    sqlDF_submission = spark.sql("SELECT title FROM comment_data JOIN submission_data ON (Replace(comment_data.link_id, 't3_', '')) = submission_data.id")
    sqlDF_submission.show() #debugging purpose
    #sqlDF_submission.write.saveAsTable("task8_timestamp")

    #3
    sqlDF_3 = spark.sql("SELECT author_flair_text as state FROM submission_data")
    sqlDF_3.show() #debuggine purpose
    #sqlDF_3.write.saveAsTable("task8_state")

#task 9


def main(context):
    """Main function takes a Spark SQL context."""
    # YOUR CODE HERE
    # YOU MAY ADD OTHER FUNCTIONS AS NEEDED
    task2()
    task4()
    task6()
    modelfit()
    task8()


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


"""
#Final Deliverable [[[JUST a ROUGH OUTLET OF CODE]]]
######## 1 ########

## R CODE FOR GRAPHICS FOR
## FINAL DELIVERABLE FOR CS143 PROJECT 2B

# If you already have R installed on your home machine, transfer the resulting files
# to your shared directory and do the visualizations in R on your home system rather than
# in the VM because R is not installed there.

# R is actually the simplest software for making plots.

##################################################
# PLOT 1: SENTIMENT OVER TIME (TIME SERIES PLOT)
###################################################

# This assumes you have a CSV file called "time_data.csv" with the columns:
# date (like 2018-08-01), Positive, Negative
# You should use the FULL PATH to the file, just in case.

time.data <- read.csv("time_data.csv", stringsAsFactors=FALSE)
time.data$date <- as.Date(time.data$date)
# Fix a small data integrity issue from the data developer.
time.data <- time.data[time.data$date != "2018-12-31", ]

# Get start and end of time series.
start <- min(time.data$date)
end <- max(time.data$date)

# Turn the data into a time series.
positive.ts <- ts(time.data$Positive, start=start, end=end)
negative.ts <- ts(time.data$Negative, start=start, end=end)

# Plot it
ts.plot(positive.ts, negative.ts, col=c("darkgreen", "red"),
        gpars=list(xlab="Day", ylab="Sentiment", main="President Trump Sentiment on /r/politics Over Time"))

################################################################
# PLOT 2: SENTIMENT BY STATE (POSITIVE AND NEGATIVE SEPARATELY)
################################################################

# May need to do
# install.packages("ggplot2)

# This assumes you have a CSV file called "state_data.csv" with the columns:
# state, Positive, Negative
# You should use the FULL PATH to the file, just in case.

library(ggplot2)
library(dplyr)

state.data <- read.csv("state_data.csv", header=TRUE)
# rename it due to the format of the state data
state.data$region <- state.data$state
chloro <- state.data %>% mutate(region=tolower(region)) %>%
    right_join(map_data("state"))

ggplot(chloro, aes(long, lat)) +
    geom_polygon(aes(group=group, fill=Positive)) +
    coord_quickmap() +
    scale_fill_gradient(low="#FFFFFF",high="#006400") +
    ggtitle("Positive Trump Sentiment Across the US")

ggplot(chloro, aes(long, lat)) +
    geom_polygon(aes(group=group, fill=Negative)) +
    coord_quickmap() +
    scale_fill_gradient(low="#FFFFFF",high="#FF0000") +
    ggtitle("Negative Trump Sentiment Across the US")

################################################################
# PLOT 3: SENTIMENT DIFF BY STATE
################################################################
chloro$Difference <- chloro$Positive - chloro$Negative
ggplot(chloro, aes(long, lat)) +
    geom_polygon(aes(group=group, fill=Difference)) +
    coord_quickmap() +
    scale_fill_gradient(low="#FFFFFF",high="#000000") +
    ggtitle("Difference in Sentiment Across the US")

##################################
# PART 4 SHOULD BE DONE IN SPARK
##################################

########################################
# PLOT 5A: SENTIMENT BY STORY SCORE
########################################
# What is the purpose of this? It helps us determine if the story score
# should be a feature in the model. Remember that /r/politics is pretty
# biased.

# Assumes a CSV file called submission_score.csv with the following coluns
# submission_score, Positive, Negative

submission.data <- read.csv("submission_score.csv", quote="", header=TRUE)
plot(Positive~story_score, data=story.data, col='darkgreen', pch='.',
     main="Sentiment By Score on Submission")
points(Negative~story_score, data=story.data, col='red', pch='.')


########################################
# PLOT 5B: SENTIMENT BY COMMENT SCORE
########################################
# What is the purpose of this? It helps us determine if the story score
# should be a feature in the model. Remember that /r/politics is pretty
# biased.

# Assumes a CSV file called comment_score.csv with the following columns
# comment_score, Positive, Negative

comment.data <- read.csv("comment_score.csv", quote="", header=TRUE)
plot(Positive~comment_score, data=comment.data, col='darkgreen', pch='.',
     main="Sentiment By Score on Comments")
points(Negative~comment_score, data=comment.data, col='red', pch='.')



###############################
# ANOTHER SAMPLE
####################################
# May first need:
# In your VM: sudo apt-get install libgeos-dev (brew install on Mac)
# pip3 install https://github.com/matplotlib/basemap/archive/v1.1.0.tar.gz

import matplotlib.pyplot as plt
import pandas as pd
import datetime
import numpy as np

from mpl_toolkits.basemap import Basemap as Basemap
from matplotlib.colors import rgb2hex
from matplotlib.patches import Polygon

"""
    IMPORTANT
    This is EXAMPLE code.
    There are a few things missing:
    1) You may need to play with the colors in the US map.
    2) This code assumes you are running in Jupyter Notebook or on your own system.
    If you are using the VM, you will instead need to play with writing the images
    to PNG files with decent margins and sizes.
    3) The US map only has code for the Positive case. I leave the negative case to you.
    4) Alaska and Hawaii got dropped off the map, but it's late, and I want you to have this
    code. So, if you can fix Hawaii and Alask, ExTrA CrEdIt. The source contains info
    about adding them back.
    """


"""
    PLOT 1: SENTIMENT OVER TIME (TIME SERIES PLOT)
    """
# Assumes a file called time_data.csv that has columns
# date, Positive, Negative. Use absolute path.

ts = pd.read_csv("time_data.csv")
# Remove erroneous row.
ts = ts[ts['date'] != '2018-12-31']

plt.figure(figsize=(12,5))
ts.date = pd.to_datetime(ts['date'], format='%Y-%m-%d')
ts.set_index(['date'],inplace=True)

ax = ts.plot(title="President Trump Sentiment on /r/politics Over Time",
             color=['green', 'red'],
             ylim=(0, 1.05))
ax.plot()

"""
    PLOT 2: SENTIMENT BY STATE (POSITIVE AND NEGATIVE SEPARATELY)
    # This example only shows positive, I will leave negative to you.
    """

# This assumes you have a CSV file called "state_data.csv" with the columns:
# state, Positive, Negative
#
# You should use the FULL PATH to the file, just in case.

state_data = pd.read_csv("state_data.csv")

"""
    You also need to download the following files. Put them somewhere convenient:
    https://github.com/matplotlib/basemap/blob/master/examples/st99_d00.shp
    https://github.com/matplotlib/basemap/blob/master/examples/st99_d00.dbf
    https://github.com/matplotlib/basemap/blob/master/examples/st99_d00.shx
    """

# Lambert Conformal map of lower 48 states.
m = Basemap(llcrnrlon=-119, llcrnrlat=22, urcrnrlon=-64, urcrnrlat=49,
            projection='lcc', lat_1=33, lat_2=45, lon_0=-95)
shp_info = m.readshapefile('/path_to/st99_d00','states',drawbounds=True)  # No extension specified in path here.
pos_data = dict(zip(state_data.state, state_data.Positive))
neg_data = dict(zip(state_data.state, state_data.Negative))

# choose a color for each state based on sentiment.
pos_colors = {}
statenames = []
pos_cmap = plt.cm.Greens # use 'hot' colormap

vmin = 0; vmax = 1 # set range.
for shapedict in m.states_info:
    statename = shapedict['NAME']
    # skip DC and Puerto Rico.
    if statename not in ['District of Columbia', 'Puerto Rico']:
        pos = pos_data[statename]
        pos_colors[statename] = pos_cmap(1. - np.sqrt(( pos - vmin )/( vmax - vmin)))[:3]
    statenames.append(statename)
# cycle through state names, color each one.

# POSITIVE MAP
ax = plt.gca() # get current axes instance
for nshape, seg in enumerate(m.states):
    # skip Puerto Rico and DC
    if statenames[nshape] not in ['District of Columbia', 'Puerto Rico']:
        color = rgb2hex(pos_colors[statenames[nshape]])
        poly = Polygon(seg, facecolor=color, edgecolor=color)
        ax.add_patch(poly)
plt.title('Positive Trump Sentiment Across the US')
plt.show()


# SOURCE: https://stackoverflow.com/questions/39742305/how-to-use-basemap-python-to-plot-us-with-50-states
# (this misses Alaska and Hawaii. If you can get them to work, EXTRA CREDIT)

"""
    PART 4 SHOULD BE DONE IN SPARK
    """

"""
    PLOT 5A: SENTIMENT BY STORY SCORE
    """
# What is the purpose of this? It helps us determine if the story score
# should be a feature in the model. Remember that /r/politics is pretty
# biased.

# Assumes a CSV file called submission_score.csv with the following coluns
# submission_score, Positive, Negative

story = pd.read_csv("submission_score.csv")
plt.figure(figsize=(12,5))
fig = plt.figure()
ax1 = fig.add_subplot(111)

ax1.scatter(story['submission_score'], story['Positive'], s=10, c='b', marker="s", label='Positive')
ax1.scatter(story['submission_score'], story['Negative'], s=10, c='r', marker="o", label='Negative')
plt.legend(loc='lower right');

plt.xlabel('President Trump Sentiment by Submission Score')
plt.ylabel("Percent Sentiment")
plt.show()

"""
    PLOT 5B: SENTIMENT BY COMMENT SCORE
    """
# What is the purpose of this? It helps us determine if the comment score
# should be a feature in the model. Remember that /r/politics is pretty
# biased.

# Assumes a CSV file called comment_score.csv with the following columns
# comment_score, Positive, Negative

story = pd.read_csv("comment_score.csv")
plt.figure(figsize=(12,5))
fig = plt.figure()
ax1 = fig.add_subplot(111)

ax1.scatter(story['comment_score'], story['Positive'], s=10, c='b', marker="s", label='Positive')
ax1.scatter(story['comment_score'], story['Negative'], s=10, c='r', marker="o", label='Negative')
plt.legend(loc='lower right');

plt.xlabel('President Trump Sentiment by Comment Score')
plt.ylabel("Percent Sentiment")
plt.show()

"""



#Create a time series plot of positive and negative sentiment. This plot should contain two lines,
# one for positive and one for negative. It must have data as an X axis and the percentage of comments
# classified as each sentiment on the Y axis.
"""
import numpy as np
from matplotlib import pyplot as plt
X, Y = np.loadtxt('examplefile.txt',
                  unpack=True,
                  delimiter=',') #you can also load CSV file as well

plt.title(" time series plot of positive and negative sentiment")
plt.xlabel("data") #label x axis
plt.ylabel("percentage of comments") #label y axix
plt.plot(X, Y) #ploting graph
plt.savefig("myfile.png") #saving file
plt.show() #showing graph


######## 2 ##########
#Create 2 maps of the United States: one for positive sentiment and one for negative sentiment. Color the states by the percentage.

#to do this we may have to use CHOROPLOT map that generate USA map based on the States


######## 3 ##########
#Create a third map of the United States that computes the difference: %Positive - %Negative.

######## 4 ##########
#Give a list of the top 10 positive stories (have the highest percentage of positive comments)
# and the top 10 negative stories (have the highest percentage of negative comments).

some_list = [-5, -1, -13, -11, 4, 8, 16, 32]
max([n for n in some_list if n<0])
#will output -1
max([n for n in some_list  if n>0])
#output 32


######## 5 ##########
#Create a scatterplot where the X axis is the Reddit score
x = [2,3,4,3,4,5]
y = [4,3,2,2,3,3]
plt.scatter(x, y, label="some label", color="k")

plt.title(" scatter graph")
plt.xlabel("Reddit Score") #label x axis
plt.ylabel("some") #Don't know if need this one??
plt.plot(X, Y) #ploting graph
plt.savefig("myfile.png") #saving file
plt.show() #showing graph





######## 6 ##########
#Any other plots that make sense will receive extra credit.

######## 7 ##########
#Extra Credit: Produce the ROC curves for YOUR classifiers and compute the Area Under the Curve for each one, which is a measure of accuracy


######## 8 ##########
#Write a paragraph summarizing your findings. What does /r/politics think about President Trump? Does this vary by state? Over time? By story/submission?
"""


