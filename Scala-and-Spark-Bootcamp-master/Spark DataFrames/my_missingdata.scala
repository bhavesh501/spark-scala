import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder().getOrCreate()
val df = spark.read.option("header","true").option("inferSchema","true").csv("ContainsNull.csv")
df.printSchema()
df.show()

//drop rows which have less than 2 null values
val drop_table = df.na.drop(2)
drop_table.show()

//filling null values of integer columns with 100
df.na.fill(100).show()
//filling null values of string columns with 100
df.na.fill("null values").show()
//filling null values of integer columns with 100 for a particular column
df.na.fill(100,Array("Sales")).show()
//replace a values
df.na.replace("Sales",Map(345 -> 62)).show()
