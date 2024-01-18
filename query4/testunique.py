from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create a Spark session
spark = SparkSession.builder.appName("ReadCSV").getOrCreate()

# Define the path to your CSV file
csvFilePath = ["hdfs://okeanos-master:54310/data/crime-data-from-2010-to-2019.csv", "hdfs://okeanos-master:54310/data/crime-data-from-2020-to-present.csv"]

# Read the CSV file into a DataFrame
df = spark.read.option("header", "true").csv(csvFilePath)

# Check for uniqueness in the "DR_NO" column
is_DR_NO_unique = df.select("DR_NO").distinct().count() == df.count()

# Print the result
if is_DR_NO_unique:
    print("The 'DR_NO' column is unique.")
else:
    print("The 'DR_NO' column is not unique.")
    print(df.count())
    print(df.select("DR_NO").distinct().count())

# Stop the Spark session
spark.stop()

