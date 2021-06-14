import json
from pyspark import SparkContext, SparkConf, SQLContext
# appName = "JSON Parse to Parquet File"
# master = "local[2]"
# conf = SparkConf().setAppName(appName).setMaster(master)
# sc = SparkContext(conf=conf)
# sqlContext = SQLContext(sc)
from pyspark.sql.functions import explode, col
from pyspark.sql.session import SparkSession

spark = SparkSession.builder \
    .appName('SparkByExamples.com') \
    .config('spark.some.config.option', 'some-value') \
    .getOrCreate()
a = "String"


# df = spark.read.option("multiline","true").format('json').load('C:/Users/arpit.kirit.shah/Desktop/data.json')
# df = df.select(explode("records").alias("records"))
# df = df.select(col("records.id").alias("records_id"),
#                col("records.data").alias("records_data"),
#                col("records.ts").alias("records_ts"))
# df.show(truncate=False)
# +-------------------------------------------------------------------------------------------------------------------------+
# |records                                                                                                                  |
# +-------------------------------------------------------------------------------------------------------------------------+
# |[[ABC, 10101, 2019-04-23T17:25:43.1112], [XYZ, 10102, 2019-04-23T18:25:43.1112], [XYZ1, 10102, 2019-04-23T18:25:43.1112]]|
# # +-------------------------------------------------------------------------------------------------------------------------+
# f = open('C:/Users/arpit.kirit.shah/Desktop/data.json')
# data = json.load(f)
# input_Data = data['persons']
# print(input_Data)
# input_df = sqlContext.read.json(sc.parallelize(input_Data))
# input_df.show(truncate=False)
# result_df = input_df.drop_duplicates(['id', 'ts'])
#
# result_df.repartition(1).write.mode('overwrite').parquet("json_output")
#
# op = sqlContext.read.parquet('json_output')
# op.show(truncate=False)

source_df = spark.read.option("multiline", "true").json('C:/Users/arpit.kirit.shah/Desktop/data.json')
# Explode all persons into different rows
persons = source_df.select(explode("persons").alias("persons"))
persons.show(truncate=False)


# Explode all car brands into different rows
persons_cars = persons.select(
   col("persons.name").alias("persons_name")
 , col("persons.age").alias("persons_age")
 , explode("persons.cars").alias("persons_cars_brands")
 , col("persons_cars_brands.name").alias("persons_cars_brand")
)

# Explode all car models into different rows
persons_cars_models = persons_cars.select(
   col("persons_name")
 , col("persons_age")
 , col("persons_cars_brand")
 , explode("persons_cars_brands.models").alias("persons_cars_model")
)
persons_cars_models.show(truncate=False)


# +------------+-----------+------------------+------------------+
# |persons_name|persons_age|persons_cars_brand|persons_cars_model|
# +------------+-----------+------------------+------------------+
# |John        |30         |Ford              |Fiesta            |
# |John        |30         |Ford              |Focus             |
# |John        |30         |Ford              |Mustang           |
# |John        |30         |BMW               |320               |
# |John        |30         |BMW               |X3                |
# |John        |30         |BMW               |X5                |
# |Peter       |46         |Huyndai           |i10               |
# |Peter       |46         |Huyndai           |i30               |
# |Peter       |46         |Mercedes          |E320              |
# |Peter       |46         |Mercedes          |E63 AMG           |
# +------------+-----------+------------------+------------------+
