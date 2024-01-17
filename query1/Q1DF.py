from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, DoubleType
from pyspark.sql.functions import col, to_date, year, month, count, rank
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("Query 1 with DataFrame API").getOrCreate()

# Reading the basic dataset 
crime = spark.read.csv(["hdfs://okeanos-master:54310/data/crime-data-from-2010-to-2019.csv",
                        "hdfs://okeanos-master:54310/data/crime-data-from-2020-to-present.csv"], header=True)

# Creating our basic DataFrame
crime_df = crime.select(
    to_date(col("Date Rptd"), "MM/dd/yyyy hh:mm:ss a").alias("Date Rptd"),
    to_date(col("DATE OCC"), "MM/dd/yyyy hh:mm:ss a").alias("DATE OCC"),
    col("Vict Age").cast(IntegerType()).alias("Vict Age"),
    col("LAT").cast(DoubleType()).alias("LAT"),
    col("LON").cast(DoubleType()).alias("LON"),
)

# We only need year and month from DATE OCC
crime_df = crime_df.withColumn("year", year("DATE OCC")).withColumn("month", month("DATE OCC"))

# Group by year and month and count the number of rows for year-month combo 
crime_df = crime_df.groupBy("year", "month").agg(count("*").alias("crime_total"))

# Keeping the top 3 months for each year using Window !!!
window = Window.partitionBy("year").orderBy(col("crime_total").desc())
crime_df = crime_df.withColumn("#", rank().over(window)).filter(col("#") <= 3).orderBy(col("year"), col("crime_total").desc())

# Keeping only the columns we need to print
crime_df = crime_df.select(
    col("year"),
    col("month"),
    col("crime_total"),
    col("#")
)

# Showing all the results 
crime_df.show(crime_df.count())
