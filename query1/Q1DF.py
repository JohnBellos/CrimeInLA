from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType, DateType, DoubleType
from pyspark.sql.functions import col, to_date, year, month, count, rank
from pyspark.sql import Window 

spark = SparkSession \
        .builder \
        .appName("Query 1 with DataFrame API") \
        .getOrCreate()

# Date format !!! 
date_format = 'MM/dd/yyyy hh:mm:ss a'

# Reading the basic dataset and creating the schema 
crime = spark.read.csv(["hdfs://okeanos-master:54310/data/crime-data-from-2010-to-2019.csv", "hdfs://okeanos-master:54310/data/crime-data-from-2020-to-present.csv"], header=True)

crime_df = crime.select(
    to_date(col("Date Rptd"), date_format).alias("Date_Rptd"),
    to_date(col("DATE OCC"), date_format).alias("DATE_OCC"),
    col("Vict Age").cast(IntegerType()).alias("Vict_Age"),
    col("LAT").cast(DoubleType()).alias("LAT"),
    col("LON").cast(DoubleType()).alias("LON"),
)

# We only need year and month from DATE OCC
crime_df = crime_df.withColumn("year", year("DATE_OCC")).withColumn("month", month("DATE_OCC"))

# Group by year and month and count the number of rows 
crime_df = crime_df.groupBy("year", "month").agg(count("*").alias("crime_total"))

# Keeping the top 3 months for each year 
window = Window.partitionBy("year").orderBy(col("crime_total").desc())
crime_df = crime_df.withColumn("#", rank().over(window))

# Keeping only the rows with rank <= 3 
crime_df = crime_df.filter(col("#") <= 3)

# Sorting by year in asc and count in desc
crime_df = crime_df.sort(col("year"), col("crime_total").desc())

# Preparing the columns we need to print
crime_df = crime_df.select(
        col("year").alias("year"),
        col("month").alias("month"),
        col("crime_total").alias("crime_total"),
        col("#").alias("#")
)

# Print 
crime_df.show(crime_df.count())
