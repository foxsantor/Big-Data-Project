from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType
import pyspark.sql.functions as Functions
from pyspark.sql.window import Window


spark = SparkSession \
    .builder \
    .appName("Spark project") \
    .getOrCreate()


sucuideData = spark.read.csv('who_suicide_statistics.csv', header=True)
#Filtering Null data
sucuideDataConverted = sucuideData.withColumn('suicides_no',sucuideData.suicides_no.cast(DoubleType()))\
.withColumn('population',sucuideData.population.cast(DoubleType()))
type(sucuideData)

#*****************get the number of all entries inside of the file*****************
print(sucuideData.count())

#*****************get all Females entries*****************
sucuideData.filter(sucuideData.sex == "female")\
    .show(truncate=False)

#*****************get all the male over the age of 75*****************
sucuideData.filter((sucuideData.sex == "male") & (sucuideData.age == "75+ years"))\
    .show(truncate=False)

#*****************get the entry of the highest number of suicide cases*****************
sucuideDataConverted.orderBy(Functions.desc('suicides_no')).limit(1).show()

#*****************get the min suicide cases of females aging between 15-24 within the year 2010*****************
sucuideDataConverted.filter((sucuideData.sex == "female") & (sucuideData.age == "15-24 years")& (sucuideData.year == "2010") & (sucuideData.suicides_no.isNotNull()))\
    .orderBy(Functions.asc('suicides_no')).show(truncate=False)

#*****************get the sum of the suicide cases for Russia*****************   
sucuideDataConverted.filter((sucuideData.country == "Russian Federation") & (sucuideData.suicides_no.isNotNull()))\
.groupBy('country')\
.agg(Functions.sum("suicides_no"))\
.show()

#*****************get the percentage of the people that died from suicide in the year of 2009*****************    
sucuideDataConverted.filter((sucuideData.year == "2009") & (sucuideData.suicides_no.isNotNull()))\
.groupBy('year')\
.agg(Functions.sum("suicides_no"),Functions.sum("population")).withColumn('percentage', (Functions.col('sum(suicides_no)')/Functions.col('sum(population)')))\
.show()

#*****************get the percentage of the people that died from suicide for each country*****************
sucuideDataConverted.filter((sucuideData.population.isNotNull())& (sucuideData.suicides_no.isNotNull()))\
.groupBy('country')\
.agg(Functions.sum("suicides_no"),Functions.sum("population")).withColumn('percentage', (Functions.col('sum(suicides_no)')/Functions.col('sum(population)')))\
.show()