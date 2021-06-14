import pyspark
import py4j
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import max, col, dense_rank, desc
from pyspark.sql.types import IntegerType, StructType

spark = SparkSession.builder \
    .appName('SparkByExamples.com') \
    .config('spark.some.config.option', 'some-value') \
    .getOrCreate()

feed_file = spark.read.csv('C:/Users/arpit.kirit.shah/Desktop/Data.csv', header=True)
print("START")
feed_file.show(truncate=False)
# Ordering the data frmae in desc order by sal
w = Window.partitionBy(feed_file.dept_name).orderBy(desc('sal'))
# casting sal to Integer type
feed_file = feed_file.withColumn('sal',
                                 col('sal').cast(IntegerType()))

# finding max sal for a particular department
# feed_file = feed_file.withColumn('DEPT_MAX_SAL',
#                                  max(col('sal')).over(w))
# finding 3 rd highest sal
feed_file = feed_file.withColumn('dense_ranks',
                                 dense_rank().over(w))
print("Number of partitions")
print(feed_file.rdd.getNumPartitions())
# Showing 3rd highest sal of each department
# +-------+---------+----+-----------+
# |dept_id|dept_name|sal |dense_ranks|
# +-------+---------+----+-----------+
# |6      |COMP     |3000|3          |
# |4      |IT       |1500|3          |
# +-------+---------+----+-----------+
dr = feed_file.filter(feed_file.dense_ranks == 3)
print("Showing 3rd highest sal of each department")
dr.show(truncate=False)

# writing dataframe to parquet format
feed_file.write.mode('overwrite').parquet("output")
# feed_file.coalesce(1).write.format("csv").option('header', 'true').mode('overwrite').save("output/df.csv")

# getting data from parquet file
data = spark.read.parquet('output')
print("Data in parquet")
data.show(truncate=False)

# Data in parquet
# +-------+---------+-----+-----------+
# |dept_id|dept_name|sal  |dense_ranks|
# +-------+---------+-----+-----------+
# |5      |COMP     |5000 |1          |
# |7      |COMP     |3500 |2          |
# |6      |COMP     |3000 |3          |
# |3      |IT       |10000|1          |
# |2      |IT       |5000 |2          |
# |4      |IT       |1500 |3          |
# |1      |IT       |1000 |4          |
# +-------+---------+-----+-----------+


# writing datafrmae to parquet using partionBy
feed_file.write.partitionBy("dept_name").mode("overwrite").parquet("paartition_output")
partitioned_data = spark.read.parquet('paartition_output/dept_name=COMP')
print("DISPLAYING COMP DEPT DATA ")
partitioned_data.show(truncate=False)

# DISPLAYING COMP DEPT DATA
# +-------+----+-----------+
# |dept_id|sal |dense_ranks|
# +-------+----+-----------+
# |5      |5000|1          |
# |7      |3500|2          |
# |6      |3000|3          |
# +-------+----+-----------+
