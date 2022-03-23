from re import S
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, when,col

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
df_geo = df_geo.na.fill(0, subset=['area_sq_km','area_rural_sq_km','area_urban_sq_km'])

#Check null values exist
df_geo.select([count(when(col(c).isNull(), c)).alias(c) for c in df_geo.columns]).show()


