from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, DoubleType
from pyspark.sql.functions import col, to_date

spark = SparkSession.builder.appName("DF from Basic Dataset").getOrCreate()

# Reading the basic dataset 
crime = spark.read.csv(["hdfs://okeanos-master:54310/data/crime-data-from-2010-to-2019.csv",
                        "hdfs://okeanos-master:54310/data/crime-data-from-2020-to-present.csv"], header=True)

# Creating our basic DataFrame
crime_df = crime.select(
    to_date(col("Date Rptd"), 'MM/dd/yyyy hh:mm:ss a').alias("Date Rptd"),
    to_date(col("DATE OCC"), 'MM/dd/yyyy hh:mm:ss a').alias("DATE OCC"),
    col("Vict Age").cast(IntegerType()),
    col("LAT").cast(DoubleType()),
    col("LON").cast(DoubleType())
)

# Getting the schema of our basic dataset (+ the total number of lines)
crime_df.printSchema()
print("Number of lines:", crime_df.count())

# Getting the data types of each column 
data_types = crime_df.dtypes

for column_name, column_type in data_types:
    print(f"Column: {column_name}, Type: {column_type}")

    