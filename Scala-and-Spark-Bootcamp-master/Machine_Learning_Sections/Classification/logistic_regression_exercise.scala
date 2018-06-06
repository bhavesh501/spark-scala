import org.apache.spark.sql.SparkSession
import org.apache.log4j._

val spark = SparkSession.builder().getOrCreate()
val data = spark.read.option("header","true").option("inferSchema","true").format("csv").load("advertising.csv")
val selected_data = (data.select(data("Clicked on Ad").as("label"),
                            $"Daily Time Spent on Site",$"Age",$"Area Income",
                            $"Daily Internet Usage",
                            $"Male"))
selected_data.printSchema()
selected_data.show(10)
val ref_data = selected_data.na.drop()
ref_data.printSchema()
ref_data.show(10)


import org.apache.spark.ml.feature.{VectorAssembler,VectorIndexer,StringIndexer,OneHotEncoder}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.Pipeline

//string encoding or factoring
//val ad_indexer = new StringIndexer().setInputCol("Ad Topic Line").setOutputCol("ad_indexed")
//val country_indexer = new StringIndexer().setInputCol("Country").setOutputCol("country_indexed")

//onehotencoding
//val ad_hot = new OneHotEncoder().setInputCol("ad_indexed").setOutputCol("ad_hot")
//val city_hots = new OneHotEncoder().setInputCol("city_indexed").setOutputCol("city_hot")
//val country_hots = new OneHotEncoder().setInputCol("country_indexed").setOutputCol("country_hot")

//using VectorAssembler
val assembler = (new VectorAssembler().setInputCols(Array("Daily Time Spent on Site","Age","Area Income",
                        "Daily Internet Usage",
                        "Male")).setOutputCol("features"))

//using LogisticRegression
val lr = new LogisticRegression()

//splitting data into training_set and test_set
val Array(training,test) = ref_data.randomSplit(Array(0.7,0.3),seed = 12345)

//using Pipeline
val pipe = (new Pipeline().setStages(Array(assembler,lr)))

//building Model
val model = pipe.fit(training)

//making predictions
val results = model.transform(test)


//Evaluating or confusion confusionMatrix
///////////Model Evaluation////////////
import org.apache.spark.mllib.evaluation.MulticlassMetrics
val  pred_label = results.select("Prediction","label").as[(Double,Double)].rdd
val metrics = new MulticlassMetrics(pred_label)
println("confusion matrix :")
println(metrics.confusionMatrix)
