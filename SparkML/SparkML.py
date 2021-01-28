from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.sql.functions import mean,col,split, col, regexp_extract, when, lit
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import QuantileDiscretizer
import pandas as pd
import numpy as np
from decimal import Decimal
from pyspark.sql.types import DoubleType,IntegerType

spark = SparkSession \
    .builder \
    .appName("Spark ML example on IMDB INDIA data ") \
    .getOrCreate()

path = "C:/Users/21650/Desktop/DATASET/IMDB_INDIA.csv"

IMDB_df = spark.read.csv(path,header = 'True',inferSchema='True')

IMDB_df.cache()
IMDB_df.show(5)


#To check the number of missing values for each column
def null_value_count(df):
  null_columns_counts = []
  numRows = df.count()
  for k in df.columns:
    nullRows = df.where(col(k).isNull()).count()
    if(nullRows > 0):
      temp = k,nullRows
      null_columns_counts.append(temp)
  return(null_columns_counts)
null_columns_count_list = null_value_count(IMDB_df)
spark.createDataFrame(null_columns_count_list, ['Column_With_Null_Value', 'Null_Values_Count']).show()



#Change the type of the columns to Double and Integer
IMDB_df=IMDB_df.withColumn("label",IMDB_df["Rating"].cast(DoubleType()).alias("label"))
IMDB_df=IMDB_df.withColumn("MetaScore",IMDB_df["MetaScore"].cast(DoubleType()).alias("MetaScore"))
IMDB_df=IMDB_df.withColumn("Vote_Count",IMDB_df["Vote_Count"].cast(IntegerType()).alias("Vote_Count"))
IMDB_df=IMDB_df.withColumn("Gross",IMDB_df["Gross"].cast(DoubleType()).alias("Gross"))
IMDB_df=IMDB_df.withColumn("Year",IMDB_df["Year"].cast(IntegerType()).alias("Year"))

#Showing the type of each feature
IMDB_df.printSchema()

#Drop the NULL values of Genre
IMDB_df = IMDB_df.dropna(subset=['Genre'])
indexers = [StringIndexer(inputCol=column, outputCol=column+"_index").setHandleInvalid("skip").fit(IMDB_df) for column in ["Genre"]]
pipeline = Pipeline(stages=indexers)
IMDB_df = pipeline.fit(IMDB_df).transform(IMDB_df)

IMDB_df.show()
IMDB_df.printSchema()
#Droping the non-essential / duplicated ( rating and genre ) features
IMDB_df = IMDB_df.drop("Id","Name","Rating","MetaScore","Genre")
#we have alot of missing values so the solution is to impute them ( by givining their min instead of a NULL value)
from pyspark.ml.feature import Imputer
imputer = Imputer(
    inputCols=IMDB_df.columns, 
    outputCols=["{}_imputed".format(c) for c in IMDB_df.columns]
)
IMDB_df=imputer.fit(IMDB_df).transform(IMDB_df)

#since that imputation creates new columns we droped the ones with missing values
IMDB_df = IMDB_df.drop('Vote_Count','Gross','Year')

IMDB_df=IMDB_df.withColumn("label",IMDB_df["label_imputed"].cast(DoubleType()).alias("label"))
print(IMDB_df.count())

#IMDB_df.na.drop("any").show(truncate=False)


IMDB_df.show()
#to optimize the training process the model only accepts vectors as training inputs
feature = VectorAssembler(inputCols=IMDB_df.drop("label_imputed").columns,outputCol="features")
feature_vector= feature.transform(IMDB_df)
#feature_vector= feature_vector.select("features")
feature_vector.show()
#Split the data 80% training and 20% Test
(trainingData, testData) = feature_vector.randomSplit([0.8, 0.2],seed = 11)


#ALGORITHM imports
#Random Forest Regression is a supervised learning algorithm that uses ensemble learning method for regression.
from pyspark.ml import Pipeline
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.feature import VectorIndexer
from pyspark.ml.evaluation import RegressionEvaluator





# Train a RandomForest model 
rf = RandomForestRegressor(featuresCol="features",maxBins=523)

# Train the model
model = rf.fit(trainingData)

# Make predictions
predictions = model.transform(testData)

# Select example rows to display.
predictions.select("prediction", "label", "features").show(20)
#Show the number of label ordered ascendingly
predictions.groupBy('label').count().orderBy('count', ascending= False).show(20)

# Select (prediction, true label) and compute test error
evaluator = RegressionEvaluator(
    labelCol="label", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictions)
#The result is  0.381336
print("Root Mean Squared Error (RMSE) on test data = %g" % rmse)

print(model)  # summary only


