from pyspark.sql import SparkSession

sc = SparkSession \
    .builder \
    .appName("RDD query 2 execution") \
    .getOrCreate() \
    .sparkContext

def get_period(hour):
    if 500 <= hour < 1200:
        return "Morning"
    elif 1200 <= hour < 1700:
        return "Afternoon"
    elif 1700 <= hour < 2100:
        return "Evening"
    else:
        return "Night"

crimes1 = sc.textFile("hdfs://okeanos-master:54310/data/crime-data-from-2010-to-2019.csv").rdd
print(crimes1.take(2))
crimes2 = sc.textFile("hdfs://okeanos-master:54310/data/crime-data-from-2020-to-present.csv").map(lambda x: (x.split(",")))

header = crimes1.first()
print(header)

crimes1 = crimes1.filter(lambda line: line != header).collect()
print(crimes1.take(2))

header = crimes2.first()
print(header)
crimes2 = crimes2.filter(lambda line: line != header).collect()
print(crimes2.take(2))

crimes = crimes1.union(crimes2)
crimes = crimes.filter(lambda line: line != header)
print(crimes.take(2))


# Apply the mapping function to the RDD
mapped_rdd = crimes1.map(lambda x: get_period(int(x[3])))

# Count the occurrences of each period
count_rdd = mapped_rdd.map(lambda x: (x[0], 1)).reduceByKey(lambda a, b: a + b)

# Sort the result in descending order based on the count
sorted_rdd = count_rdd.sortBy(lambda x: x[0], ascending=False)

# Collect and print the result
result = sorted_rdd.collect()
for row in result:
    print(row)
