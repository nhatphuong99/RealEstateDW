

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("TestPySparkLocal") \
    .master("local[*]") \
    .getOrCreate()

print("Spark version:", spark.version)

data = [("Quan 1", 120000000), ("Quan 7", 85000000), ("Thu Duc", 60000000)]
df = spark.createDataFrame(data, schema=["quan", "gia_m2"])
df.show()

spark.stop()