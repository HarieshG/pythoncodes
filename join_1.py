import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Join_1').getOrCreate()
print(spark)