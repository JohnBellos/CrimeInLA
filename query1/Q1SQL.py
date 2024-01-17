from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, DoubleType
from pyspark.sql.functions import col, to_date, year, month, count, rank

spark = SparkSession \
    .builder \
    .appName("Query 1 with SQL API") \
    .getOrCreate()

# Reading the basic dataset and creating the schema
crime = spark.read.csv(["hdfs://okeanos-master:54310/data/crime-data-from-2010-to-2019.csv", "hdfs://okeanos-master:54310/data/crime-data-from-2020-to-present.csv"], header=True)

crime_sql = crime.select(
    to_date(col("Date Rptd"), 'MM/dd/yyyy hh:mm:ss a').alias("Date Rptd"),
    to_date(col("DATE OCC"), 'MM/dd/yyyy hh:mm:ss a').alias("DATE OCC"),
    col("Vict Age").cast(IntegerType()).alias("Vict Age"),
    col("LAT").cast(DoubleType()).alias("LAT"),
    col("LON").cast(DoubleType()).alias("LON"),
)

# Creating a view of the dataset
crime_sql.createOrReplaceTempView("crimes")

# Finally good, old SQL queries :')
query1 = """
    SELECT Year AS year, Month AS month, Count as crime_total, Rank AS `#`
        FROM ( 
            SELECT Year, Month, Count, RANK() OVER (PARTITION BY Year ORDER BY Count DESC) AS Rank FROM (SELECT YEAR(`DATE OCC`) AS Year, MONTH(`DATE OCC`) AS Month, COUNT(*) AS Count
            FROM crimes
            GROUP BY Year, Month
            )
        )
    WHERE Rank <= 3
    ORDER BY year, crime_total DESC
"""

# Let's execute the query and print 
q1 = spark.sql(query1)
q1.show(q1.count())
