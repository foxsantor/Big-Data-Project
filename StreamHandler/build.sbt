name := "Stream Handler"

version := "1.0"

libraryDependencies ++= Seq(
	"org.apache.spark" %% "spark-core" % "2.4.7" % "provided",
	"org.apache.spark" %% "spark-sql" % "2.4.7" % "provided",   
)
