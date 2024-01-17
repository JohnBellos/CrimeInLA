from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, DoubleType
from pyspark.sql.functions import col, to_date, year, count, regexp_replace, when 

spark = SparkSession \
        .builder \
        .appName("Query 3 with DataFrame API") \
        .getOrCreate()

# Reading the income dataset
income_2k15 = spark.read.csv("hdfs://okeanos-master:54310/data/LA_income_2015.csv", header=True)

# Filtering (only Los Angeles communities) 
income_2k15 = income_2k15.filter(col("Community").like("Los Angeles%"))

# Removing dollar signs and commas, and casting the column to DoubleType for sorting
income = income_2k15.withColumn("Estimated Median Income", regexp_replace(col("Estimated Median Income"), "[^0-9.]", "").cast(DoubleType()))

# Reading the reverse geocoding dataset
rev_geo = spark.read.csv("hdfs://okeanos-master:54310/data/revgecoding.csv", header=True)

# Reading the basic dataset and creating a dataframe 
crime_2015 = spark.read.csv(["hdfs://okeanos-master:54310/data/crime-data-from-2010-to-2019.csv", "hdfs://okeanos-master:54310/data/crime-data-from-2020-to-present.csv"], header=True)

crime_2015 = crime_2015.select(
    to_date(col("DATE OCC"), "MM/dd/yyyy hh:mm:ss a").alias("DATE OCC").cast(DateType()),
    col("LAT").cast(DoubleType()),
    col("LON").cast(DoubleType()),
    col("Vict Descent").cast(StringType())
)

# We only need 2015 data
crime_2015 = crime_2015.filter(year("DATE OCC") == 2015)
crime_vics = crime_2015.select("LAT", "LON", "Vict Descent")

# Filtering out the victimless crimes 
crime_vics = crime_vics.filter(col("Vict Descent").isNotNull())

# Joining REVGEO + CRIME on lat and lon 
join1 = crime_vics.join(rev_geo,on=["LAT", "LON"], how="inner")
join1 = join1.withColumnRenamed("ZIPcode", "Zip Code")

# Joining JOIN1 + INCOME on zip codes 
join2 = income.join(join1.select("Zip Code"), on="Zip Code", how="inner")
join2 = join2.drop_duplicates(subset=['Zip Code'])

# Order by ZIP CODES and keep top3 & bottom3
top3zip = join2.orderBy(col("Estimated Median Income").desc()).limit(3)
bottom3zip = join2.orderBy(col("Estimated Median Income")).limit(3)

# Joining JOIN1 + zips
top3 = join1.join(top3zip, on=["Zip Code"], how="inner")
bottom3 = join1.join(bottom3zip, on=["Zip Code"], how="inner")

# Selecting only Vict Descent column 
top_3_victims = top3.select("Vict Descent")
bottom_3_victims = bottom3.select("Vict Descent")

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

# Grouping by Vict Descent, counting the victims and getting the results in desc 
rich_victims = top_3_victims.groupBy("Vict Descent").agg(count("*").alias("#")).orderBy(col("#").desc())
rich_victims = rich_victims.withColumnRenamed("Vict Descent", "Victim Descent")

poor_victims = bottom_3_victims.groupBy("Vict Descent").agg(count("*").alias("#")).orderBy(col("#").desc())
poor_victims = poor_victims.withColumnRenamed("Vict Descent", "Victim Descent")

# Printing the final results for each group 
print("Rich victims")
rich_victims.show()

print("Poor victims")
poor_victims.show()
