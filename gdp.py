from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Top GDP growth each year") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

df = spark.read.load("gdp.csv",format="csv", header = True)

df.createOrReplaceTempView("gdp")

tempDF=spark.sql("SELECT Year,`Country Name`,Value FROM gdp")
tempDF.createOrReplaceTempView('tempDF')

sqlDF1 = spark.sql("SELECT A.`Country Name`, A.Year,A.Value,((A.Value-B.Value)/B.Value) as gdpValue FROM tempDF A JOIN tempDF B ON A.`Country Name` = B.`Country Name` AND A.Year-1=B.Year")
sqlDF1.createOrReplaceTempView('sqlDF1')
sqlDF2 = spark.sql("SELECT Year,MAX(gdpValue) as maxgdpValue FROM sqlDF1 GROUP BY Year")
sqlDF2.createOrReplaceTempView('sqlDF2')

sqlDF = spark.sql("SELECT A.Year, A.`Country Name` FROM sqlDF1 A JOIN sqlDF2 B ON A.Year=B.Year and A.gdpValue=B.maxgdpValue ORDER BY Year ASC")
sqlDF.show(sqlDF.count(),False)
spark.stop()



# sqlDF= spark.sql("SELECT Year,max(Value) as maxval From gdp Group By Year Order By Year DESC")
# sqlDF.createOrReplaceTempView('sqlDF')
# sqlDF.printSchema()
# # sqlDF.show(sqlDF.count(),False)
# sqlDF2= spark.sql("SELECT Year,`Country Name`,Value FROM gdp")
# sqlDF2.createOrReplaceTempView('sqlDF2')
# sqlDF2.printSchema()
# # result = spark.sql("SELECT A.Year,B.`Country Name` FROM sqlDF2 B, sqlDF A WHERE A.max(Value)=B.Value")
# result = spark.sql("SELECT A.Year,A.`Country Name` FROM sqlDF2 A join sqlDF B ON B.maxval=A.Value")

# # sqlDF.show(sqlDF.count(),False)
# result.show(result.count(),False)
