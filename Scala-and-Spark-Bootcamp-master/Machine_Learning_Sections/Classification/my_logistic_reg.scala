import org.apache.spark.sql.SparkSession
import org.apache.log4j._
Logger.getLogger("org").setLevel(Level.ERROR)

val spark = SparkSession.builder().getOrCreate()
//importing data
val data = spark.read.option("header","true").option("inferSchema","true").format("csv").load("titanic.csv")
data.printSchema()

val data1 = data.select($"PassengerId",data("Survived").as("label"),$"Pclass",$"Sex",$"Age",$"SibSp",$"Parch",$"Fare",$"Embarked")

val ref_data = data1.na.drop()
ref_data.show(10)

import org.apache.spark.ml.feature.{VectorAssembler,StringIndexer,OneHotEncoder,VectorIndexer}
import org.apache.spark.ml.linalg.Vectors

//convert strings into numerical values
val sexIndexer = new StringIndexer().setInputCol("Sex").setOutputCol("sexIndex")
val embarkIndexer = new StringIndexer().setInputCol("Embarked").setOutputCol("embarkIndex")

//convert numberical values into one hot encoding
val genderEncoder = new OneHotEncoder().setInputCol("sexIndex").setOutputCol("sexEncode")
val embarkEncoder = new OneHotEncoder().setInputCol("embarkIndex").setOutputCol("embarkEncode")

//VectorAssembler
val assembler = (new VectorAssembler().setInputCols(Array("PassengerId","Pclass","sexEncode",
                    "embarkEncode","Age","SibSp","Parch","Fare")).setOutputCol("features"))

//splitting of ref_data into training_data and test_data
val Array(training_data,test_data) = ref_data.randomSplit(Array(0.7,0.3),seed = 12345)
//val train = assembler.transform(training_data).select(training_data("Survived").as("label"),$"features")
//val test = assembler.transform(test_data).select(test_data("Survived").as("label"),$"features")


import org.apache.spark.ml.classification.LogisticRegression
val lr = new LogisticRegression()

import org.apache.spark.ml.Pipeline
val pipe = new Pipeline().setStages(Array(sexIndexer,embarkIndexer,genderEncoder,embarkEncoder,assembler,lr))

//building LogisticRegression model
val model = pipe.fit(training_data)

//predicting results
val results = model.transform(test_data)
results.printSchema()

///////////Model Evaluation////////////
import org.apache.spark.mllib.evaluation.MulticlassMetrics
val  pred_label = results.select("Prediction","label").as[(Double,Double)].rdd
val metrics = new MulticlassMetrics(pred_label)
println("confusion matrix :")
println(metrics.confusionMatrix)
