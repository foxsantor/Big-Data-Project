from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType
import pyspark.sql.functions as Functions
from pyspark.sql.window import Window
from pyspark.ml.regression import LinearRegression
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.ml.clustering import BisectingKMeans
from pyspark.ml.evaluation import ClusteringEvaluator
import numpy as np

spark = SparkSession \
    .builder \
    .appName("spark ml for CO2 emissions in Canada dataset") \
    .getOrCreate()


CO2Data = spark.read.csv('CO2 Emissions_Canada.csv',inferSchema=True, header=True)

CO2Data.show()
CO2Data.printSchema()

#cleaning the DF from rows containing null values
CO2Data_C_DF = CO2Data.na.drop()

CO2Data_O_DF = CO2Data_C_DF.groupBy("Make","Vehicle_Class").agg(Functions.sum("Cylinders").alias("Cylinders"),Functions.sum("Engine_Size_L").alias("Engine_Size_L")).orderBy("Make","Vehicle_Class", ascending=True)
CO2Data_O_DF = sucuideData.na.drop()



#more cleaning to the colums
EXTR_CLDT = [Functions.col(column).alias(column.replace(' ', '_')) for column in CO2Data_O_DF.columns]
#
tdata = orgnizedDF.select(*EXTR_CLDT).selectExpr("oneElementVec(Engine_Size_L) as features", "Cylinders as label")
tdata.show()
#spliting 80% to train and 20% to the test process
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
CR = BisectingKMeans().setK(2).setSeed(1)
model = CR.fit(tdata)

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