from pyspark.sql import SparkSession, Window
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType, DateType, DoubleType
from pyspark.sql.functions import col, to_date, year, month, count, rank, regexp_replace, format_number, when, lit, udf, avg, round, row_number
from math import radians, sin, cos, sqrt, atan2

# Define the Haversine formula as a UDF
def haversine_udf(lat1, lon1, lat2, lon2):
    R = 6371.0  # Earth's radius in kilometers

    lat1 = radians(lat1)
    lon1 = radians(lon1)
    lat2 = radians(lat2)
    lon2 = radians(lon2)

    dlon = lon2 - lon1
    dlat = lat2 - lat1

    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    distance = R * c

    return distance

# Initialize SparkSession
spark = SparkSession \
        .builder \
        .appName("Query 4 with DataFrame API") \
        .getOrCreate()

# Reading the basic dataset 
crime = spark.read.csv(["hdfs://okeanos-master:54310/data/crime-data-from-2010-to-2019.csv", "hdfs://okeanos-master:54310/data/crime-data-from-2020-to-present.csv"], header=True)

# Keeping what we need :)
crime_df = crime.select(
    col("DR_NO"),
    to_date(col("DATE OCC"), 'MM/dd/yyyy hh:mm:ss a').alias("DATE OCC"),
    col("AREA ").cast(IntegerType()).alias("AREA"),
    col("LAT").cast(DoubleType()).alias("LAT"),
    col("LON").cast(DoubleType()).alias("LON"),
    col("Weapon Used Cd").cast(IntegerType()).alias("Weapon Used Cd")
)

crime_df = crime_df.withColumn("year", year("DATE OCC"))

crime_df = crime_df.select(
    col("DR_NO"),
    col("year"),
    col("AREA"),
    col("LAT"),
    col("LON"),
    col("Weapon Used Cd")
)

# Filtering out the Null Island 
crimes_df = crime_df.filter((col("LAT") != 0) & (col("LON") != 0) & col("Weapon Used Cd").isNotNull())

# crimes_df.show()
# print(crimes_df.count())

# Reading the dataset with the precincts  
LAPD_stations = spark.read.csv("hdfs://okeanos-master:54310/data/LAPD_Police_Stations.csv", header=True)

# Our police stations schema :)
stations_df = LAPD_stations.select(
    col("Y").cast(DoubleType()),
    col("X").cast(DoubleType()),
    col("FID").cast(IntegerType()),
    col("DIVISION").cast(StringType()).alias("division"),
    col("LOCATION").cast(StringType()),
    col("PREC").cast(StringType())
)

# stations_df.show()


# Define the UDF
haversine_udf = udf(haversine_udf, DoubleType())

# Joining the 2 datasets 
join2 = crimes_df.crossJoin(stations_df).withColumn(
    "Distance", haversine_udf(col("lat"), col("lon"), col("y"), col("x"))
) 

# join2.show()

# Use window function to find the nearest police station for each crime
windowSpec = Window.partitionBy("DR_NO").orderBy("Distance")
join2 = join2.withColumn("rn", row_number().over(windowSpec)).filter(col("rn") == 1).drop("rn")

# Format the Distance column to three decimal places
join2 = join2.withColumn("Distance", format_number(col("Distance").cast(DoubleType()), 3))

# Show the resulting DataFrame
# join2.show()

# Group by year and calculate count of crimes and average distance
res2 = join2.groupBy("division") \
    .agg(
        round(avg("Distance"), 3).alias("average_distance"),
        count("Weapon Used Cd").alias("#"),
    ) \
    .orderBy("division")

res2.show()