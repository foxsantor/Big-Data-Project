import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.types._


object streamHandler {
	def main(args: Array[String]) {

		//structure of the messeges comming from kafka Server 
		val columns: Seq[String] = Seq("country","year","sex","age","suicides_no","population")

		//spark session
		val spark = SparkSession
			.builder
			.appName("Stream Handler")
			.getOrCreate()

		//to show errors coming from spark 	
		import spark.implicits._

		// read from Kafka
		val inputDF = spark
			.readStream
			.format("kafka") // org.apache.spark:spark-sql-kafka
			.option("kafka.bootstrap.servers", "localhost:9092")//local kafka server binded with zookeeper
			.option("subscribe", "test")//name of the topic emitted by Kafka broker
			.load()

			//mapping the data from the json produced by Kafka producer 
			val exprs = columns.map(c => get_json_object($"value", s"$$.$c").as(c))
			//building the raw dataframe while converting from binary type to String 
			val rawDF = inputDF.selectExpr("CAST(value AS STRING)").select(exprs: _*)
			rawDF.createOrReplaceTempView("suicide_data")
			
			//*************A group by using countries and countring cases of suicides*********************** 
			//val alteredDF = rawDF.groupBy("country").count()

			//*****************get the number of all entries inside of the file*****************
			//val alteredDF = spark.sql("SELECT COUNT (*) AS total FROM suicide_data")

			//*****************get all Females entries*****************
			//val alteredDF = spark.sql("SELECT * FROM suicide_data WHERE sex == 'female'")

			//*****************get all the male over the age of 75*****************
			//val alteredDF = spark.sql("SELECT * FROM suicide_data WHERE age == '75+ years' AND sex == 'male'")

			//*****************get foreach country the highest number of cases *****************
			//val alteredDF = spark.sql("SELECT country, MAX(suicides_no) as Max_No_Cases FROM suicide_data group by country")

			//*****************get the min suicide cases of females aging between 15-24 within the year 2010*****************
			//val alteredDF = spark.sql("SELECT sex,age, MIN(suicides_no) as Min_No_Cases FROM suicide_data where sex =='female' and age =='15-24 years' group by sex,age ")

			//*****************get the sum of the suicide cases for Russia*****************   
			//val alteredDF = spark.sql("SELECT country, SUM(suicides_no) as Sum_Cases FROM suicide_data where country =='Russian Federation' group by country ")
			
			//*****************get the percentage of the people that died from suicide in the year of 2009***************** 
			//val alteredDF = spark.sql("SELECT year, sum(suicides_no) as sum_cases , sum(population) as sum_pop, ROUND( sum(suicides_no) * 100.0 / sum(population), 1) AS Percentage FROM suicide_data where year =='2009' group by year ")
			
			//*****************get the percentage of the people that died from suicide for each country*****************
			val alteredDF = spark.sql("SELECT country, sum(suicides_no) as sum_cases , sum(population) as sum_pop, ROUND( sum(suicides_no) * 100.0 / sum(population), 1) AS Percentage FROM suicide_data  group by country ")
			
			val query = alteredDF
		  	.writeStream
         	.outputMode("update")
			.format("console")
			.option("numRows", "1000") 
         	.start()

		query.awaitTermination()

	}
}

