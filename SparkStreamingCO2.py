#!/usr/bin/env python
# coding: utf-8

# In[ ]:



from pyspark.sql import SparkSession
from pyspark.sql.types import StructType


# In[15]:


spark = SparkSession.builder.appName("FirasChikhaouiapp").getOrCreate()


# In[16]:


userSchema = StructType().add("make", "string").add("model", "string").add("vehicle_class", "string").add("engine_size", "float").add("cylinders", "integer").\
add("transmission", "string").add("fuel_type", "string").add("fuel_con_city", "float").add("fuel_con_hwy", "float").add("fuel_con_comb", "float").\
add("fuel_con_comb_mpg", "float").add("co2_emission", "integer")


# In[42]:


dfCSV = spark.readStream.option("sep", ",").option("header", "true").schema(userSchema).csv("c:/tmp/hive")


# In[43]:


dfCSV.createOrReplaceTempView("Co2Emission")
# *****************get all entries  ***************
result = spark.sql("select count(*) from Co2Emission")
# *****************get top 10 entries that has engine size below 1.5   ***************
result = spark.sql("select make,model,engine_size from Co2Emission where engine_size < 1.5 limit(10)")
query = result.writeStream.outputMode("complete").format("console").start()

# ***************** get top 10 entries that has fuel type Z(Premium gasoline)   ***************

result = spark.sql("select make,model,fuel_type from Co2Emission where `fuel_type` = 'Z' limit(10) ")
query = result.writeStream.outputMode("append").format("console").start()

# *****************get top 10 BMW cars sorted by CO2 Emission   ***************

result = spark.sql("select make,model,fuel_type,co2_emission from Co2Emission where `make` = 'BMW' order by co2_emission limit(10) ")
# query = result.writeStream.outputMode("append").format("console").start()

# ***************** Ordering is done after the grouping fuel_con_city that are below 7 is completed, so I can sort by the results of those aggregate functions ***************
result = spark.sql("select `fuel_con_city` ,count (*) as count Co2Emission where `fuel_con_city` < 7 group by 'fuel_con_city' order by `count` limit(10)")
# query = result.writeStream.outputMode("append").format("console").start()

# ***************** Ordering is done after the grouping fuel_con_hwy that are below 7 is completed, so I can sort by the results of those aggregate functions  ***************
result = spark.sql("select `fuel_con_hwy` ,count (*) as count Co2Emission where `fuel_con_hwy` < 7 group by 'fuel_con_hwy' order by `count` limit(10)")
query = result.writeStream.outputMode("complete").format("console").start()

# ***************** Ordering is done after the grouping cylinders that are EQUAL 4 is completed, so I can sort by the results of those aggregate functions   ***************
result = spark.sql("select `cylinders` ,count (*) as count Co2Emission where `cylinders` = 4 group by 'cylinders' order by `count` limit(10)")
query = result.writeStream.outputMode("complete").format("console").start()

# ***************** Ordering is done after the grouping engine sizes between 50 and 100  is completed, so I can sort by the results of those aggregate functions   ***************
result = spark.sql("select count(*) as count from Co2Emission where `engine_size` between 50 and 100 'Graphic Design' group by `level`")
query = result.writeStream.outputMode("complete").format("console").start()


query = result.writeStream.outputMode("complete").format("console").start()
query.awaitTermination()








