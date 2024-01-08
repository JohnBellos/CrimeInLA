from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

# Create a Spark configuration and set the app name
conf = SparkConf().setAppName("RDD query 2 execution")
sc = SparkContext(conf=conf)

# Create a SQLContext to work with Spark SQL
sqlContext = SQLContext(sc)

# Specify the path to your CSV file
csv_file_path_1 = "hdfs://okeanos-master:54310/data/crime-data-from-2010-to-2019.csv"
csv_file_path_2 = "hdfs://okeanos-master:54310/data/crime-data-from-2020-to-present.csv"

# Use the Spark SQL context to read the CSV file into a DataFrame
rdd1 = sqlContext.read.format("com.databricks.spark.csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(csv_file_path_1) \
    .rdd

rdd2 = sqlContext.read.format("com.databricks.spark.csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(csv_file_path_2) \
    .rdd

rdd = rdd1.union(rdd2)

rdd = rdd.filter(lambda row: row[15] == 'STREET')
# Show the DataFrame
# df.show()
print(rdd.take(3))

# Define a function to categorize the time into periods
def categorize_time(row):
    time_occ = int(row[3])
    if 500 <= time_occ < 1200:
        return "Morning"
    elif 1200 <= time_occ < 1700:
        return "Afternoon"
    elif 1700 <= time_occ < 2100:
        return "Evening"
    else:
        return "Night"

# Use map to categorize the time and create key-value pairs
categorized_rdd = rdd.map(lambda row: (categorize_time(row), 1))

# Use reduceByKey to count occurrences for each period
count_by_period = categorized_rdd.reduceByKey(lambda a, b: a + b)

# Use sortBy to rank periods from most to least occurrences
sorted_periods = count_by_period.sortBy(lambda x: x[1], ascending=False)

# Collect the results
result = sorted_periods.collect()

# Display the result
for period, count in result:
    print(f"{period}: {count} occurrences")
# Stop the SparkContext
sc.stop()

