# import pyspark
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, udf, overlay, translate
# from pyspark.sql.types import StringType
#
# spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
#
# columns = ["Seqno", "Name"]
# data = [("1", "john jones"),
#         ("2", "tracey smith"),
#         ("3", "amy sanders")]
#
# df = spark.createDataFrame(data=data, schema=columns)
# df.select(translate)
#
# df.show(truncate=False)
#
#
# def convertCase(str):
#     resStr = ""
#     arr = str.split(" ")
#     for x in arr:
#         resStr = resStr + x[0:1].upper() + x[1:len(x)] + " "
#     return resStr
#
#
# """ Converting function to UDF """
# convertUDF = udf(lambda z: convertCase(z))
#
# df.select(col("Seqno"), convertUDF(col("Name")).alias("Name")) \
#     .show(truncate=False)
#
#
# def upperCase(str):
#     return str.upper()
#
#
# upperCaseUDF = udf(lambda z: upperCase(z), StringType())
#
# df.withColumn("Cureated Name", upperCaseUDF(col("Name"))) \
#     .show(truncate=False)
#
# """ Using UDF on SQL """
# spark.udf.register("convertUDF", convertCase, StringType())
# df.createOrReplaceTempView("NAME_TABLE")
# spark.sql("select Seqno, convertUDF(Name) as Name from NAME_TABLE") \
#     .show(truncate=False)
#
# spark.sql("select Seqno, convertUDF(Name) as Name from NAME_TABLE " + \
#           "where Name is not null and convertUDF(Name) like '%John%'") \
#     .show(truncate=False)
#
# """ null check """
#
# columns = ["Seqno", "Name"]
# data = [("1", "john jones"),
#         ("2", "tracey smith"),
#         ("3", "amy sanders"),
#         ('4', None)]
#
# df2 = spark.createDataFrame(data=data, schema=columns)
# df2.show(truncate=False)
# df2.createOrReplaceTempView("NAME_TABLE2")
#
# spark.udf.register("_nullsafeUDF", lambda str: convertCase(str) if not str is None else "", StringType())
#
# spark.sql("select _nullsafeUDF(Name) from NAME_TABLE2") \
#     .show(truncate=False)
#
# spark.sql("select Seqno, _nullsafeUDF(Name) as Name from NAME_TABLE2 " + \
#           " where Name is not null and _nullsafeUDF(Name) like '%John%'") \
#     .show(truncate=False)
if __name__ == '__main__':
    n = int(input())
    l = []
    for _ in range(n):
        s = input().split()
        cmd = s[0]
        args = s[1:]

        if cmd != "print":
            cmd += "(" + ",".join(args) + ")"
            print(cmd)

            eval("l." + cmd)
        else:
            print(l)