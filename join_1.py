# Created on 23/03/2021
# @author: tailwyndz LLC
# Confidential and Proprietary. Copyright (c) by Tailwyndz LLC. All Rights Reserved
# Packages: pyspark, pyspark sql function, pyspark ML features


import dis
from re import S
import pyspark
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, when,col,expr, udf, avg,to_date,regexp_replace,last,lpad,concat_ws,date_format,year,month
from  pyspark.sql.types import IntegerType, DecimalType, DateType
from pyspark.ml.feature import Imputer
from pyspark.sql import Window
import sys


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
 #----------------------Geography---------------------

#Reading geography dataset
df_geo = spark.read.format('csv').option('header',True).option('inferSchema',True).load("wasbs://datasets@trainingbatchaccount.blob.core.windows.net/geography.csv")

#Drop openstreetmapid & elevation_m
df_geo = df_geo.drop('elevation_m','openstreetmap_id')

#Fill null values with zero
df_geo = df_geo.na.fill(0, subset=['area_sq_km','area_rural_sq_km','area_urban_sq_km','latitude','longitude'])

#----------------------Demographics---------------------

#Reading demographics dataset
df_demo = spark.read.format('csv').option('header',True).option('inferSchema',True).load("wasbs://datasets@trainingbatchaccount.blob.core.windows.net/demographics.csv")

#fill null population with sum of male & female population
addpopulation = when(col("population").isNull(), (col("population_male") + col("population_female"))).otherwise(col("population"))
df_demo = df_demo.withColumn("population", addpopulation)

#Fill null values with zero
df_demo = df_demo.na.fill(0, subset=['population_rural', 'population_clustered','population_urban','population_largest_city','population_largest_city','population_density','human_development_index',	'population_age_00_09','population_age_10_19',	'population_age_20_29',	'population_age_30_39',	'population_age_40_49'	,'population_age_50_59'	,'population_age_60_69',	'population_age_70_79'	,'population_age_80_and_older'])

#sum of rows
df_value = df_demo.agg(avg(df_demo.population_male), avg(df_demo.population_female)).collect()
v_avg = df_value[0][0]/df_value[0][1]
ratio = v_avg.as_integer_ratio()
div_value = ratio[0] + ratio[1]

#fill null population_male 
addpopulation_male = when(col("population_male").isNull(), (col("population") / div_value)*ratio[0]).otherwise(col("population_male"))
df_demo = df_demo.withColumn("population_male", addpopulation_male)

#fill null population_female 
addpopulation_female = when(col("population_female").isNull(), (col("population") / div_value)*ratio[1]).otherwise(col("population_female"))
df_demo = df_demo.withColumn("population_female", addpopulation_female)

#----------------------Join_1---------------------
df_join_1 = df_demo.join(df_geo, df_geo.location_key == df_demo.location_key,'inner').drop(df_demo.location_key)

#----------------------Economy---------------------
df_eco = spark.read.format('csv').option('header',True).option('inferSchema',True).load("wasbs://datasets@trainingbatchaccount.blob.core.windows.net/economy.csv")

#fill null with zero
df_eco = df_eco.na.fill(0)

#----------------------Join_2---------------------
df_join_2 = df_join_1.join(df_eco, df_eco.location_key == df_join_1.location_key,'inner').drop(df_eco.location_key)


#----------------------Health---------------------
df_health = spark.read.format('csv').option('header',True).option('inferSchema',True).load("wasbs://datasets@trainingbatchaccount.blob.core.windows.net/health.csv")

# filling the null values in the life_expectancy column with the mean of it
imputer = Imputer(inputCol='life_expectancy',outputCol="life_expectancy").setStrategy("mean")
health_df = imputer.fit(df_health).transform(df_health)

#----------------------Join_3---------------------
df_join_3 = df_join_2.join(df_health, df_health.location_key == df_join_2.location_key,'inner').drop(df_health.location_key)

#----------------------Epidemiology---------------------
df_epidemiology = spark.read.format('csv').option('header',True).option('inferSchema',True).load("wasbs://datasets@trainingbatchaccount.blob.core.windows.net/epidemiology.csv")

df_epidemiology = df_epidemiology.withColumn('date',to_date(df_epidemiology['date'],format='yyyy-mm-dd'))

df_epidemiology = df_epidemiology.na.fill(value=0)


#---------------------------------Opening government response dataset and cleaning it-------------------

df_gr = spark.read.format('csv').option('header',True).option('inferSchema', True).load("wasbs://datasets@trainingbatchaccount.blob.core.windows.net/governmentResponse.csv")

#removing the rows which have null values for all the columns
df_gr = df_gr.na.drop(how = "all", thresh = None, subset= None )

#replacing null values with 0 for columns having integer values
df_gr = df_gr.na.fill(value = 0)

#replacing null values with empty string "" for columns having string values
df_gr = df_gr.na.fill("")

#formatting date

df_gr = df_gr.withColumn('date',to_date(df_gr['date'],'yyyy-mm-dd'))

#---------------------------------Opening emergency decleration dataset and cleaning it----------------------

df_ed = spark.read.format('csv').option('header',True).option('inferSchema', True).load("wasbs://datasets@trainingbatchaccount.blob.core.windows.net/emergencydec.csv")

df_ed = df_ed.na.drop(how = "all", thresh = None, subset = None )

columns_not_to_cast = ["date", "location_key", "lawatlas_mitigation_policy"]
#change to float
df_ed = (
   df_ed
   .select(
     *(c for c in columns_not_to_cast),
     *(col(c).cast("float").alias(c) for c in df_ed.columns if c not in columns_not_to_cast)
   )
)
#replace null with 0.5 neither true nor false
df_ed = df_ed.na.fill(value = 0.5)

df_ed = df_ed.na.fill("")

#df_ed = df_ed.withColumn('date',to_date(df_ed['date'],format='yyyy-mm-dd'))
df_ed = df_ed.withColumn('date',to_date(df_ed['date'],'yyyy-mm-dd'))

#deleting empty columns
df_ed = df_ed.drop('lawatlas_requirement_type_traveler_must_self_quarantine', 'lawatlas_requirement_type_traveler_must_inform_others_of_travel', 'lawatlas_requirement_type_checkpoints_must_be_established', 'lawatlas_requirement_type_travel_requirement_must_be_posted', 'lawatlas_business_type_non_essential_retail_businesses', 'lawatlas_business_type_all_non_essential_businesses')

#-------------------------------Joining emergency declaration and government response datasets-----------------------------

df_join_5 = df_gr.join(df_ed, on = ['date', 'location_key'],how =  'leftouter').drop(df_ed.date).drop(df_ed.location_key)
df_join_5 = df_join_5.na.drop(how = "all", thresh = None, subset = None )
df_join_5 = df_join_5.na.fill(value = 0)
df_join_5 = df_join_5.na.fill("")

#---------------------------------Cleaning Index dataset---------------------------------------------------------------------
df_index = spark.read.format('csv').option('header',True).option('inferSchema',True).load("wasbs://datasets@trainingbatchaccount.blob.core.windows.net/index.csv")
df_index = df_index.select("location_key", "country_code","country_name", "subregion1_code", "subregion1_name")
df_index = df_index.na.fill("")
#---------------------------------Opening  weather dataset and cleaning it----------------------

df_weather = spark.read.format('csv').option('header',True).option('inferSchema',True).load("wasbs://datasets@trainingbatchaccount.blob.core.windows.net/weather.csv")
df_weather = df_weather.withColumn('Date',to_date(df_weather['Date'],'yyyy-mm-dd'))
df_weather = df_weather.drop('snowfall_mm')
df_weather = df_weather[df_weather.Date > "2019-12-31"]


#---------------------------------Opening hospitalization dataset and cleaning it-------------------

df_hos = spark.read.format('csv').option('header',True).option('inferSchema',True).load("wasbs://datasets@trainingbatchaccount.blob.core.windows.net/hospitalizations.csv")
#Date format
df_hos = df_hos.withColumn('Date',to_date(df_hos['Date'],'dd-mm-yyyy'))
#fill empty values with zero
df_hos = df_hos.fillna(value = 0, subset = ['current_hospitalized_patients','current_intensive_care_patients','new_ventilator_patients','cumulative_ventilator_patients','current_ventilator_patients'])
#drop unwanted column
df_hos = df_hos.drop('cumulative_ventilator_patients','new_ventilator_patients')
#get only 2020 data
df_hos = df_hos[df_hos.Date > "2019-12-31"]


#-------------------------------Joining Weather and hospitalization datasets-----------------------------
df_join_6 = df_weather.join(df_hos, on = ['Date', 'location_key'],how =  'leftouter').drop(df_hos.Date).drop(df_hos.location_key)

#------------------------------Joining Join_5 & Join_6----------------------------------------------------
df_join_7 = df_join_6.join(df_join_5, on = ['Date', 'location_key'],how =  'leftouter').drop(df_hos.Date).drop(df_hos.location_key)

#------------------------------Joining Join_7 & Epidemology----------------------------------------------------

df_join_8 = df_epidemiology.join(df_join_7, on = ['Date', 'location_key'],how =  'leftouter').drop(df_join_7.Date).drop(df_join_7.location_key)

#-----------------------------------Vaccination------------------------------------------------------------------
#Reading vaccination data
df_vaccination = spark.read.format('csv').option('header',True).option('inferSchema',True).load("wasbs://datasets@trainingbatchaccount.blob.core.windows.net/vaccinations.csv")

#selecting needed columns only
df_vaccination = df_vaccination.select("date","location_key","new_persons_vaccinated","cumulative_persons_vaccinated","new_persons_fully_vaccinated","cumulative_persons_fully_vaccinated","new_vaccine_doses_administered","cumulative_vaccine_doses_administered")

#Drop rows if all the values are null
df_vaccination = df_vaccination.na.drop(subset=["new_persons_vaccinated","cumulative_persons_vaccinated","new_persons_fully_vaccinated","cumulative_persons_fully_vaccinated","new_vaccine_doses_administered","cumulative_vaccine_doses_administered"] ,how="all")

#fill cumulative value with previous field value (Forward Fill)
df_vaccination = df_vaccination.withColumn("cumulative_persons_vaccinated", last('cumulative_persons_vaccinated', True).over(Window.partitionBy('location_key').rowsBetween(-sys.maxsize, 0)))
df_vaccination = df_vaccination.withColumn("cumulative_persons_fully_vaccinated",last('cumulative_persons_fully_vaccinated', True).over(Window.partitionBy('location_key').rowsBetween(-sys.maxsize, 0)))
df_vaccination = df_vaccination.withColumn("cumulative_vaccine_doses_administered",last('cumulative_vaccine_doses_administered', True).over(Window.partitionBy('location_key').rowsBetween(-sys.maxsize, 0)))

#replace null with zero
df_vaccination = df_vaccination.na.fill(value=0)

#correct date format
df_vaccination = df_vaccination.withColumn('Date',to_date(df_vaccination['Date'],format='yyyy-mm-dd'))

#------------------------------Joining Join_8 & Vaccination----------------------------------------------------

df_join_9 = df_join_8.join(df_vaccination, on = ['Date', 'location_key'],how =  'leftouter').drop(df_vaccination.Date).drop(df_vaccination.location_key)

#------------------------------Joining Join_3 & Join_9----------------------------------------------------

df_join_10 = df_join_9.join(df_join_3, on = ['location_key'],how =  'leftouter').drop(df_join_3.location_key)

#-------------------------------Joining Join_10 & Index----------------------------------------------------
df_join_11 = df_join_10.join(df_index, on = ['location_key'],how =  'leftouter').drop(df_index.location_key)
df_join_11.printSchema()
#--------------------------------Commerical Aviation-------------------------------------------------------------
#Reading vaccination data
df_com_avi = spark.read.format('csv').option('header',True).option('inferSchema',True).load("wasbs://datasets@trainingbatchaccount.blob.core.windows.net/Commercial_Aviation_Departures.csv")

# Change date format from 02/12/2022 to 02-12-2022
df_com_avi = df_com_avi.withColumn('Date', regexp_replace('Date', '/', '-'))
df_com_avi = df_com_avi.withColumn('Date',to_date(df_com_avi['Date'],format='MM-dd-yyyy'))
df_com_avi = df_com_avi.select('Mode', 'Indicator' ,'Date','Lowest','Current','Last Year')
df_com_avi = df_com_avi.groupBy('Mode','Indicator',month('Date'), year('Date')).sum()
df_com_avi = df_com_avi.withColumnRenamed('month(Date)','Month').withColumnRenamed('Year(Date)','Year').withColumnRenamed('sum(Lowest)','Lowest').withColumnRenamed('sum(Current)','Current').withColumnRenamed('sum(Last Year)', 'LastYear')

df_com_avi = df_com_avi.select((lpad(df_com_avi.Month, 2, '0').alias('Month')), 'YEAR','MODE','Indicator','Lowest','Current','LastYear')

 # concats the month and year to single date column
df_com_avi = df_com_avi.select(concat_ws('', df_com_avi['Month'], df_com_avi['Year']).alias('Date'), df_com_avi["*"])


 # convers the date column's type from string to date
df_com_avi = df_com_avi.withColumn('Date', to_date(df_com_avi['Date'], format='MMyyyy'))

df_com_avi = df_com_avi.drop('Month','Year')
df_com_avi = df_com_avi.na.fill(value=0)


#----------------------------Monthly Aviation--------------------------------------------------------------------
df_mon_avi = spark.read.format('csv').option('header', True).option('inferSchema', True).load("wasbs://datasets@trainingbatchaccount.blob.core.windows.net/monthly_aviation.csv")
# converts the month from single digit to double digit
df_mon_avi = df_mon_avi.select((lpad(df_mon_avi.Month, 2, '0').alias('Month')), "Year", "DOMESTIC", "INTERNATIONAL", "TOTAL")

# concats the month and year to single date column
df_mon_avi = df_mon_avi.select(concat_ws('', df_mon_avi.Month, df_mon_avi.Year).alias('Date'), df_mon_avi["*"])

# convers the date column's type from string to date
df_mon_avi = df_mon_avi.withColumn('Date', to_date(df_mon_avi['Date'], format='MMyyyy'))

# selects four major needed data columns from the data frame
df_mon_avi = df_mon_avi.select("Date", "DOMESTIC", "INTERNATIONAL", "TOTAL")



#----------------------------------Monthly Transportation---------------------------------------
#reading monthly transportation statistics data
df_mt = spark.read.format('csv').option('header',True).option('inferSchema',True).load("wasbs://datasets@trainingbatchaccount.blob.core.windows.net/Monthly_Transportation_Statistics.csv")

#removing "." from column names (presence of "." in the column names throws error)
new_col = []
for c in df_mt.columns:
        a = c.replace(".","")
        a = a.replace("(","")
        a = a.replace(")","")
        new_col.append(a)

df_mt = df_mt.toDF(*new_col)

#dropping unnecessary column
df_mt = df_mt.drop('Index')

#changing date format 
df_mt = df_mt.na.drop(how = "all", thresh = None, subset = None )
df_mt = df_mt.na.fill(value = 0)
df_mt = df_mt.withColumn('Date', regexp_replace('Date', '/', '-'))
df_mt = df_mt.withColumn('Date', regexp_replace('Date',  ' 12:00:...', ''))
df_mt = df_mt.withColumn('Date', regexp_replace('Date',  'AM', ''))
# df_mt = df_mt.withColumn('Date', to_date(df_mt['Date'],'dd-mm-yyyy'))
# UDF to convert string to date

func =  udf (lambda x: datetime.strptime(x, '%m-%d-%Y'), DateType())

df_mt = df_mt.withColumn('Date', date_format(func(col('Date')), 'yyyy-MM-dd'))


#--------------------------------------------Join Monthly aviation & Monthly Transportation---------------------------
df_ma_mt = df_mon_avi.join(df_mt,on=['Date'],how='inner').drop(df_mt.Date)


#---------------------------------------------Join_MA_MT & Commercial aviation----------------------------------------
df_join_12 = df_ma_mt.join(df_com_avi,on=['Date'],how='inner').drop(df_com_avi.Date)

#---------------------------------------------Final JOIN----------------------------------------
df_final = df_join_11.join(df_join_12,on=['Date'],how='leftouter').drop(df_join_12.Date)
df_final = df_final.na.fill("")
df_final = df_final.na.fill(value=0)
df_final.show()
df_final.printSchema()