from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("carros_postgres").getOrCreate()

# Read the CSV file into a DataFrame
df = spark.read.options(header='True', inferSchema='True', 
delimiter=',').csv("cars.csv")

# Define your PostgreSQL connection properties
postgres_properties = {
    "user": "postgres",
    "password": "spark",
    "url": "jdbc:postgresql://db:5432/hive-metastore",
    "driver": "org.postgresql.Driver",
}

# Define the target PostgreSQL table
postgres_table = "minhatabela"

# Write the DataFrame into the PostgreSQL table
df.write.jdbc(
    url=postgres_properties["url"],
    table=postgres_table,
    mode="overwrite",  # You can use "append" or "ignore" if needed
    properties=postgres_properties,
)

# Stop the Spark session
spark.stop()

print("Seed completed")
