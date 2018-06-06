import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder().getOrCreate()
val df = spark.read.option("header","true").option("inferSchema","true").csv("Sales.csv")

df.printSchema()
df.show()

//using groupby and aggregrate
df.groupBy("company").mean().show()

df.groupBy("company").max().show()
df.groupBy("company").min().show()
df.groupBy("company").sum().show()

df.select(sum("sales")).show()
df.select(countDistinct("sales")).show()
df.select(sumDistinct("sales")).show()
df.select(variance("sales")).show()
df.select(stddev("sales")).show()
df.select(collect_set("sales")).show()

//using orderBy
df.orderBy("sales").show()

//to use descending we have to use scala notation
df.orderBy($"sales".desc).show()
