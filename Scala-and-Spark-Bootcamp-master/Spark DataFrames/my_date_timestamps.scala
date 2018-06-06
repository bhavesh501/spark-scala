import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder.getOrCreate()
val df = spark.read.option("header","true").option("inferSchema","true").csv("CitiGroup2006_2008")
df.printSchema()
df.show()

//showing a particular column
df.select("Date").show()

//extracting month from Date
df.select(month(df("Date"))).show()

//extracting year from Date
df.select(year(df("Date"))).show()

//extracting mean of close by year
//creating a new column of years
val df2 = df.withColumn("Year",year(df("Date")))

println("df2")
df2.show()

val dfavgs = df2.groupBy("Year").mean() //creates the column in the name of avg(close)

println("dfavgs")
dfavgs.show()

dfavgs.select("Year","avg(Close)").show() //dfavgs.select($"Year",$"avg(Close)").show()

//getting min of close by years
val dfmins = df2.groupBy("Year").min()

println("dfmins")
dfmins.show()

dfmins.select($"Year", $"min(Close)").show()
