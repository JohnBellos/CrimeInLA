from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType, DateType, DoubleType
from pyspark.sql.functions import col, to_date, year, month, count, rank

spark = SparkSession \
    .builder \
    .appName("Query 1 with SQL API") \
    .getOrCreate()

# Date format !!!
date_format = 'MM/dd/yyyy hh:mm:ss a'

# Reading the basic dataset and creating the schema
crimes = spark.read.csv(["hdfs://okeanos-master:54310/data/crime-data-from-2010-to-2019.csv", "hdfs://okeanos-master:54310/data/crime-data-from-2020-to-present.csv"], header=True)

crime = crimes.select(
    to_date(col("Date Rptd"), date_format).alias("Date Rptd"),
    to_date(col("DATE OCC"), date_format).alias("DATE OCC"),
    col("Vict Age").cast(IntegerType()).alias("Vict Age"),
    col("LAT").cast(DoubleType()).alias("LAT"),
    col("LON").cast(DoubleType()).alias("LON"),
)

jan_2010_crimes = crime.filter((year("DATE OCC") == 2010) & (month("DATE OCC") == 1))

total_crimes_jan_2010 = jan_2010_crimes.count()

print("Total crimes in January 2010!:", total_crimes_jan_2010)
