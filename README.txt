This Project is divided into two Parts :

First part : casual queries using spark and a csv file under the file Spark.py submit it to spark while uncommenting the lines to see the various changes.

Second part : consists of getting the csv data from a kafka producer then streaming the data live while applying queries utilizing spark streaming.

KAFKA PRODUCER -----> SPARK STREAMING (using SCALA) ------>GETTING THE DATAFRAME TO THE CONSOLE (we could even send it to a database and treat the whole process as a data pipeline)

Requirements :

Java 8(jdk 1.8),
Apache spark 2.4.7,
Hadoop for windows;
Apache ZooKeeper,
Apache Kafka,
Python 3.8,
and
sbt 2.13.4 (Scala)

Choice of IDE : VS code you can use intelliJ alternatively 

PS: most of the work is done through the cmd.

Installing the core needs for kafka can be achieved following this guide : https://dzone.com/articles/running-apache-kafka-on-windows-os .

After successfully running the producer for kafka run the script SucideProducer.py using "python SucideProducer.py" it will emit all the entries in the csv file to the kafka server.
then cd to StreamHandler where the scala project is located and use the command : 

spark-submit --class "streamHandler" --master local[*] --packages "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.7" target/scala-2.12/stream-handler_2.12-1.0.jar

*** this command  contain the version of spark which has to be 2.4.7 or lower (not 2.2) or 3.0.1 as the project isn’t compatible with the newest some of the process went deprecated 

Make sure everything is in place if the jar was not found in the target folder, while you in the StreamHandler folder use the command : sbt package to build the jar make sure everything is structured as it is DO NOT CHANGE FOLDERS OR PLACEMENT.

Each time you need to build the project using the sbt package command as running all the queries is resource consuming because the data is being streamed and altered real time.
 


