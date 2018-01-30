import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder().getOrCreate()

val df = spark.read.option("header","true").option("inferschema","true").csv("CitiGroup2006_2008")

df.printSchema()

//to use scala syntax
import spark.implicits._
//scala filtering
df.filter($"close" < 480).show()

//spark filtering
df.filter("close < 480").show()

//filtering multiple columns using scala
df.filter($"close" < 480 && $"high" < 480).show()

//filtering multiple columns using spark
df.filter("close < 480 AND high < 480").show()

//collecting data
val ch_low = df.filter("close < 480 and high < 480").collect()

for(c <- ch_low){
  println(c)
}

//taking the count
val ch_count = df.filter("close < 480 and high < 480").count()

// "=="problem in scala 2.0 instead use "==="
df.filter($"High"===484.40).show()
