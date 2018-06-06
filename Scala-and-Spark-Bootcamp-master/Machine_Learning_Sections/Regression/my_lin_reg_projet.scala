import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.regression.LinearRegression
import org.apache.log4j._
Logger.getLogger("org").setLevel(Level.ERROR)

val spark = SparkSession.builder().getOrCreate()
val data = spark.read.option("header","true").option("inferSchema","true").csv("Ecommerce Customers")
data.show(10)
data.describe()
data.printSchema()

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors

val df = data.select(data("Yearly Amount Spent").as("label"),$"Avg Session Length",$"Time on App",$"Time on Website",$"Length of Membership")
val assembler = (new VectorAssembler().setInputCols(Array("Avg Session Length","Time on App","Time on Website","Length of Membership")).setOutputCol("features"))
val input = assembler.transform(df).select($"label",$"features")

val lr = new LinearRegression()
val lrModel = lr.fit(input)
val training_summary = lrModel.summary
training_summary.predictions.show()
