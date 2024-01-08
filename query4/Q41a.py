from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType, DateType, DoubleType
from pyspark.sql.functions import col, to_date, year, month, count, rank, regexp_replace, format_number, when, lit, udf, avg, round
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
    to_date(col("DATE OCC"), 'MM/dd/yyyy hh:mm:ss a').alias("DATE OCC"),
    col("AREA ").cast(IntegerType()).alias("AREA"),
    col("LAT").cast(DoubleType()).alias("LAT"),
    col("LON").cast(DoubleType()).alias("LON"),
    col("Weapon Used Cd").cast(IntegerType()).alias("Weapon Used Cd")
)

crime_df = crime_df.withColumn("year", year("DATE OCC"))

crime_df = crime_df.select(
    col("year"),
    col("AREA"),
    col("LAT"),
    col("LON"),
    col("Weapon Used Cd")
)

# Filtering out the Null Island and keeping only firearms :')
crimes_df = crime_df.filter((col("LAT") != 0) & (col("LON") != 0) & (col("Weapon Used Cd").startswith("1")))

# crimes_df.show()
# print(crimes_df.count())

# Reading the dataset with the precincts  
LAPD_stations = spark.read.csv("hdfs://okeanos-master:54310/data/LAPD_Police_Stations.csv", header=True)

# Our police stations schema :)
stations_df = LAPD_stations.select(
    col("Y").cast(DoubleType()),
    col("X").cast(DoubleType()),
    col("FID").cast(IntegerType()),
    col("DIVISION").cast(StringType()),
    col("LOCATION").cast(StringType()),
    col("PREC").cast(StringType())
)

# stations_df.show()

# Joining the 2 datasets 
join1 = crimes_df.join(
    stations_df,
    (crime_df["AREA"] == stations_df["PREC"]),
    "inner"
)

# Define the UDF
haversine_udf = udf(haversine_udf, DoubleType())

# Apply the UDF to calculate distance and create a new column
join1 = join1.withColumn("Distance", haversine_udf(col("LAT"), col("LON"), col("Y"), col("X")))

# Show the resulting DataFrame
# join1.show()

# Group by year and calculate count of crimes and average distance
res1 = join1.groupBy("year") \
    .agg(
        round(avg("Distance"), 3).alias("average_distance"),
        count("Weapon Used Cd").alias("#"),
    ) \
    .orderBy("year")

# Show the resulting DataFrame
res1.show()