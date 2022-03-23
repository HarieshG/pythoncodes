from re import S
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, when,col,expr, udf
from  pyspark.sql.types import IntegerType

# -----------------------------FUNCTIONS-----------------------------------------------

#function to summation
def summation(*arguments):
    total = 0
    for number in arguments:
        total += number
    return total

#function for displaying NULL Count
def displayNullCount(df):
    df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns]).show()

#-----------------------------CODE---------------------------------------------------

STORAGEACCOUNTURL = "https://trainingbatchaccount.blob.core.windows.net"
STORAGEACCOUNTKEY = "2QPPHsAtQ8/fh33VE7wqg/ZaeJoxdq/pnevAEmCh0n32tC5eXa8dTEEwMHdD9Ff5k1/wVh97aubqgKzQSwOLnQ=="
CONTAINERNAME = "datasets"

spark = SparkSession.builder.appName('Join_1').getOrCreate()
spark.conf.set(
        "fs.azure.account.key.trainingbatchaccount.blob.core.windows.net",
        STORAGEACCOUNTKEY
)
#Reading geography dataset
df_geo = spark.read.format('csv').option('header',True).option('inferSchema',True).load("wasbs://datasets@trainingbatchaccount.blob.core.windows.net/geography.csv")

#Drop openstreetmapid & elevation_m
df_geo = df_geo.drop('elevation_m','openstreetmap_id')

#Fill null values with zero
df_geo = df_geo.na.fill(0, subset=['area_sq_km','area_rural_sq_km','area_urban_sq_km','latitude','longitude'])

#Reading demographics dataset
df_demo = spark.read.format('csv').option('header',True).option('inferSchema',True).load("wasbs://datasets@trainingbatchaccount.blob.core.windows.net/demographics.csv")

#fill null population with sum of male & female population
addpopulation = when(col("population").isNull(), (col("population_male") + col("population_female"))).otherwise(col("population"))
df_demo = df_demo.withColumn("population", addpopulation)

#Fill null values with zero
df_demo = df_demo.na.fill(0, subset=['population_rural', 'population_urban','population_largest_city','population_largest_city','population_density','human_development_index',	'population_age_00_09','population_age_10_19',	'population_age_20_29',	'population_age_30_39',	'population_age_40_49'	,'population_age_50_59'	,'population_age_60_69',	'population_age_70_79'	,'population_age_80_and_older'])
