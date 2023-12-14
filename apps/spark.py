import findspark
findspark.init()

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master('local[*]') \
    .appName("Iniciando com Spark") \
    .config('spark.ui.port', '4050') \
    .getOrCreate()

df = spark.read.csv("cars.csv", header=True, inferSchema=True)
df.show()

