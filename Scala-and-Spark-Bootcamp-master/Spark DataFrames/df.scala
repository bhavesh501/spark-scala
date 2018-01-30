import org.apache.spark.sql.SparkSession

//creating or getting current spark session
val spark = SparkSession.builder().getOrCreate()

//reading or loadind csv file
//val df = spark.read.csv("CitiGroup2006_2008")
val df = spark.read.option("header", "true").option("inferSchema","true").csv("CitiGroup2006_2008")
df.columns

//printing the top 5 rows
df.head(5)

//printing line by line
for(row <- df.head(5)){
  println(row)
}

//shows summary
df.describe().show()

//shows the column data
df.select($"open",$"high").show()

//create new columns based on old columns
val df2 = df.withColumn("HighPlusLow", df("High")+df("Low"))

//renaming the columns
val df3 = df2.select(df2("HighPlusLow").as("HPL")).show()
