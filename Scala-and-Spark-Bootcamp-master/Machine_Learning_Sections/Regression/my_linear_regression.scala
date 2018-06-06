import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.regression.LinearRegression

//for less warnings and errors
import org.apache.log4j._
Logger.getLogger("org").setLevel(Level.ERROR)
val spark = SparkSession.builder().getOrCreate()
val data = spark.read.option("header","true").option("inferSchema","true").format("csv").load("Clean-USA-Housing.csv")
data.show(3)
data.printSchema()

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors

val df = (data.select(data("Price").as("label"),
            $"Avg Area Income",$"Avg Area House Age",
            $"Avg Area Number of Rooms",$"Avg Area Number of Bedrooms",
            $"Area Population"))

val assembler = (new VectorAssembler().setInputCols(Array("Avg Area Income","Avg Area House Age",
                  "Avg Area Number of Rooms","Avg Area Number of Bedrooms",
                  "Area Population")).setOutputCol("features"))

val output = assembler.transform(df).select($"label",$"features")
output.show()

val lr = new LinearRegression()

val lrModel = lr.fit(output)

val training_summary = lrModel.summary
training_summary.residuals.show()
training_summary.predictions.show()
