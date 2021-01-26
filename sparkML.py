import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType
import pyspark.sql.functions as Functions
from pyspark.sql.window import Window
from pyspark.ml.regression import LinearRegression 
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.ml.clustering import BisectingKMeans
from pyspark.ml.evaluation import ClusteringEvaluator


spark = SparkSession \
    .builder \
    .appName("Spark ML project") \
    .getOrCreate()


sucuideData = spark.read.csv('who_suicide_statistics.csv',inferSchema=True, header=True)

sucuideData.show()
sucuideData.printSchema()
#Linear Regression Model
#the objective of LinearRegression algo is to predict future labels after feeding it features as inputs unfortuntly my dataframe isn't that intrestting from the value side   
#cleaning the DF from rows containing null values 
#label is the suicide cases and the feature is the population 
cleanDF = sucuideData.na.drop()

#tried to use a more reasnoable dataframe but the numbers were so big that the regressor couldn't do the training therefore i used the the initial DF 
orgnizedDF = cleanDF.groupBy("country","year")\
.agg(Functions.sum("suicides_no").alias("suicides_no"),Functions.sum("population").alias("population"))\
.orderBy("country","year", ascending=True)
orgnizedDF = sucuideData.na.drop()
#more cleaning to the colums
exprs = [Functions.col(column).alias(column.replace(' ', '_')) for column in orgnizedDF.columns]
#Declaring UDT vectors 
spark.udf.register("oneElementVec", lambda d: Vectors.dense([d]), returnType=VectorUDT())
#
tdata = orgnizedDF.select(*exprs).selectExpr("oneElementVec(population) as features", "suicides_no as label")
tdata.show()
#spliting 75% to train and 25% to the test process
train_data,test_data= tdata.randomSplit([0.75,0.25])
#init the regressor 
regressor = LinearRegression(featuresCol = "features",labelCol="label")
#predefining the model 
model= regressor.fit(train_data)
#evaluting the model using the test portion
pred_res = model.evaluate(test_data)
#showing the evaluation
pred_res.predictions.orderBy("label",ascending = False).show()

#********************clustering***************
# Trains a bisecting k-means model.
bkm = BisectingKMeans().setK(2).setSeed(1)
model = bkm.fit(tdata)

# Make predictions
predictions = model.transform(tdata)

# Evaluate clustering by computing Silhouette score
evaluator = ClusteringEvaluator()

silhouette = evaluator.evaluate(predictions)
print("Silhouette with squared euclidean distance = " + str(silhouette))

# Shows the result.
print("Cluster Centers: ")
centers = model.clusterCenters()
for center in centers:
    print(center)