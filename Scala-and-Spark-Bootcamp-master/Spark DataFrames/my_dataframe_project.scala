import org.apache.spark.sql.SparkSession
import spark.implicits._

val spark = SparkSession.builder().getOrCreate()
val df = spark.read.option("header","true").option("inferSchema","true").csv("Netflix_2011_2016.csv")
df.show()
df.printSchema()

//first 5 columns
for(col <- df.head(5)){
  println(col)
}

//describe
df.describe()

//new column HV Ratio
val df2 = df.withColumn("HV Ratio", df("High")/df("Volume"))
df2.show()

//highest high price
val dfhigh = df2.orderBy($"High".desc).show(1)

//mean of close columns
val mean_close = df2.select(mean("Close")).show()

//min and max of volume columns
val vol_max = df2.select(max("volume")).show()
val vol_min = df2.select(min("volume")).show()

//number of days close is lower than $600
val count_close = df2.filter($"close" < 600).count()
println(count_close)

//percentage of time high greater than 500$
val total_high_count = df2.select("High").count()
val count_high = df2.filter($"high" > 500).count()
println(s"percentage of time high greater than 500 is ${count_high*100/total_high_count}%")

//max high per year
val df3 = df2.withColumn("year", year(df("Date")))
df3.show()
df3.groupBy("year").max().show()

//avg close of each calender month
val df4 = df2.withColumn("month", month(df("Date")))
val monthavgs = df4.select("month","close").groupBy("month").mean()
monthavgs.orderBy("month").show()
