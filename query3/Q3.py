from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType, DateType, DoubleType
from pyspark.sql.functions import col, to_date, year, month, count, rank, regexp_replace, format_number, when 

spark = SparkSession \
        .builder \
        .appName("Query 3 with DataFrame API") \
        .getOrCreate()

# Reading the income dataset
income_2k15 = spark.read.csv("hdfs://okeanos-master:54310/data/LA_income_2015.csv", header=True)

# Filtering (only Los Angeles communities) 
income_2k15 = income_2k15.filter(col("Community").like("Los Angeles%"))

# Removing dollar signs and commas, and casting the column to DoubleType for sorting
income_clean = income_2k15.withColumn("Estimated Median Income", regexp_replace(col("Estimated Median Income"), "[^0-9.]", "").cast(DoubleType()))

# Keeping only the zipcodes for the 3 richest 
top_3_zipcodes = income_clean.sort(col("Estimated Median Income").desc()).select("Zip Code").limit(3)

# Keeping only the zipcodes for the 3 poorest 
bottom_3_zipcodes = income_clean.sort(col("Estimated Median Income")).select("Zip Code").limit(3)

# Reading the reverse geocoding dataset
rev_geo = spark.read.csv("hdfs://okeanos-master:54310/data/revgecoding.csv", header=True)

top_3_geo = top_3_zipcodes.join(
    rev_geo.hint("BROADCAST"),  # or hint("MERGE") / hint("SHUFFLE_HASH") / hint("SHUFFLE_REPLICATE_NL")
    top_3_zipcodes["Zip Code"] == rev_geo["ZIPcode"],
    how="inner"
)

bottom_3_geo = bottom_3_zipcodes.join(
    rev_geo.hint("BROADCAST"),  # or hint("MERGE") / hint("SHUFFLE_HASH") / hint("SHUFFLE_REPLICATE_NL")
    bottom_3_zipcodes["Zip Code"] == rev_geo["ZIPcode"],
    how="inner"
)

# Keeping only LAT, LON columns 
top_3_geo = top_3_geo.select("LAT", "LON")
bottom_3_geo = bottom_3_geo.select("LAT", "LON")

# # Get the execution plan text
# top_3_geo.explain()
# bottom_3_geo.explain()

# Prints for debugging 
# print("Joined data ZIP codes --> LAT, LON for the rich")
# top_3_geo.show(top_3_geo.count())

# print("Joined data ZIP codes --> LAT, LON for the poor")
# bottom_3_geo.show(bottom_3_geo.count())

# Reading the basic dataset
crime = spark.read.csv("hdfs://okeanos-master:54310/data/crime-data-from-2010-to-2019.csv", header=True)

crime_2015 = crime.select(
    to_date(col("DATE OCC"), "MM/dd/yyyy hh:mm:ss a").alias("DATE OCC").cast(DateType()),
    col("LAT").cast(DoubleType()),
    col("LON").cast(DoubleType()),
    col("Vict Descent").cast(StringType())
)

# We only need 2015 data
crime_2015 = crime_2015.filter(year("DATE OCC") == 2015)

crime_vics = crime_2015.select("LAT", "LON", "Vict Descent")

# Filtering out the victimless crimes 
crime_vics = crime_vics.na.drop(subset=["Vict Descent"])

# crime_vics.show()

top_3_join = crime_vics.join(
    top_3_geo,
    (crime_vics["LAT"] == top_3_geo["LAT"]) & (crime_vics["LON"] == top_3_geo["LON"]),
    "inner"
)

bottom_3_join = crime_vics.join(
    bottom_3_geo,
    (crime_vics["LAT"] == bottom_3_geo["LAT"]) & (crime_vics["LON"] == bottom_3_geo["LON"]),
    "inner"
)

top_3_victims = top_3_join.select("Vict Descent")
bottom_3_victims = bottom_3_join.select("Vict Descent")

# top_3_victims.show(top_3_victims.count())
# bottom_3_victims.show(bottom_3_victims.count())

# Mapping the letters in Vict Descent
descent_mapping = {
    'A': 'Other Asian',
    'B': 'Black',
    'C': 'Chinese',
    'D': 'Cambodian',
    'F': 'Filipino',
    'G': 'Guamanian',
    'H': 'Hispanic/Latin/Mexican',
    'I': 'American Indian/Alaskan Native',
    'J': 'Japanese',
    'K': 'Korean',
    'L': 'Laotian',
    'O': 'Other',
    'P': 'Pacific Islander',
    'S': 'Samoan',
    'U': 'Hawaiian',
    'V': 'Vietnamese',
    'W': 'White',
    'X': 'Unknown',
    'Z': 'Asian Indian'
}

for letter, word in descent_mapping.items():
    top_3_victims = top_3_victims.withColumn("Vict Descent", when(col("Vict Descent") == letter, word).otherwise(col("Vict Descent")))

for letter, word in descent_mapping.items():
    bottom_3_victims = bottom_3_victims.withColumn("Vict Descent", when(col("Vict Descent") == letter, word).otherwise(col("Vict Descent")))

rich_victims = top_3_victims.groupBy("Vict Descent").agg(count("*").alias("#")).orderBy(col("#").desc())
rich_victims = rich_victims.withColumnRenamed("Vict Descent", "Victim Descent")

poor_victims = bottom_3_victims.groupBy("Vict Descent").agg(count("*").alias("#")).orderBy(col("#").desc())
poor_victims = poor_victims.withColumnRenamed("Vict Descent", "Victim Descent")

rich_victims.show()
poor_victims.show()