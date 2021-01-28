#!/usr/bin/env python
# coding: utf-8

# In[ ]:



from pyspark.sql import SparkSession
from pyspark.sql.types import StructType


# In[14]:



# In[13]:



# In[15]:


spark = SparkSession.builder.appName("SparkStreaming").getOrCreate()


# In[16]:


userSchema = StructType().add("Id", "string").add("Name", "string").add("Rating", "float").add("MetaScore", "float").add("Vote_Count", "float").add("Genre", "string").add("Gross", "string").add("Year", "float")


# In[42]:


dfCSV = spark.readStream.option("sep", ",").option("header", "true").schema(userSchema).csv("C:/tmp/hive")


# In[43]:


dfCSV.createOrReplaceTempView("IMDB_INDIA")
# result = dfCSV.orderBy("Rating").limit(1)
# *****************get all entries  ***************
# result = spark.sql("select * from IMDB_INDIA")

# *****************get all entries from  Year 2020 ***************
# result = spark.sql("select * from IMDB_INDIA where Year == 2020.0")
#query = result.writeStream.outputMode("append").format("console").start()

# *****************get the entry of the highest rating movie*****************
# result = spark.sql("select `name`,`Rating` from IMDB_INDIA group by `name`,`Rating` order by `Rating` DESC limit(1)")
# query = result.writeStream.outputMode("complete").format("console").start()

# *****************get the number of drama movies *****************
# result = spark.sql("select count(*) as NbrDramaMovies from IMDB_INDIA where `Genre` = 'Drama            ' ")
# query = result.writeStream.outputMode("complete").format("console").start()

# ****************get all entries from  movies that starts with the letter A  ***************
# result = spark.sql("select * from IMDB_INDIA where `Name` like ('A%') ")
# query = result.writeStream.outputMode("append").format("console").start()

# *****************get the first entry of an Action Movie that starts with 'The'  *****************
# result = spark.sql("select * from IMDB_INDIA where `Name` like ('The%') And `Genre` = 'Action            ' limit(1)")
# query = result.writeStream.outputMode("append").format("console").start()

# *****************get the percentage of Action movies in 2020 that were rated 8 or more entries *****************
# result = spark.sql("select sum(Rating)/sum(Year) as SumRatingYear from IMDB_INDIA where `Year`== 2020.0 And `Genre` = 'Action            ' And `Rating` >= 8.0")
# query = result.writeStream.outputMode("complete").format("console").start()

# *****************get the sum of scores of all Drama, Action and Sci-Fi movies in 2019  ****************
# result = spark.sql("select sum(Rating) as SumRating from IMDB_INDIA where `Year`== 2019.0 And `Genre` = 'Action; Drama; Sci-Fi            '")
# query = result.writeStream.outputMode("complete").format("console").start()

# In[45]:







# In[ ]:


query.awaitTermination()


# In[ ]:




