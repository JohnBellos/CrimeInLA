from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType, DateType, DoubleType
from pyspark.sql.functions import col, udf, desc


spark = SparkSession \
        .builder \
        .appName("Query 2 with DataFrame API") \
        .getOrCreate()

crime = spark.read.csv(["hdfs://okeanos-master:54310/data/crime-data-from-2010-to-2019.csv", "hdfs://okeanos-master:54310/data/crime-data-from-2020-to-present.csv"], header=True)

crime = crime.filter(col("Premis Desc") == "STREET")

crime_df = crime.select(
    col("Vict Age").cast(IntegerType()).alias("Vict_Age"),
    col("LAT").cast(DoubleType()).alias("LAT"),
    col("LON").cast(DoubleType()).alias("LON"),
    col("TIME OCC").cast(IntegerType()).alias("TIME_OCC"),
)

# Define a function to map time to periods of the day
def get_period(hour):
    if 500 <= hour < 1200:
        return "Morning"
    elif 1200 <= hour < 1700:
        return "Afternoon"
    elif 1700 <= hour < 2100:
        return "Evening"
    else:
        return "Night"

get_period_udf = udf(get_period, StringType())

# Create a new column 'Period' based on the 'Time' column
crime_df = crime_df.withColumn("Period", get_period_udf(col("TIME_OCC").cast(IntegerType())))

# Group by the 'Period' column and count the rows for each period
result = crime_df.groupBy("Period").count()

result = result.orderBy(desc("count"))

# Show the result
result.show()
