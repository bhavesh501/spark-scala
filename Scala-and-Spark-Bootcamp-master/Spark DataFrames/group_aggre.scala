import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder().getOrCreate()
val df = spark.read.options("header","true").options("inferSchema","true").csv("Sales.csv")
