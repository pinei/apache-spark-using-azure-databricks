# Databricks notebook source
import glob
parts = glob.glob('/dbfs/databricks-datasets/airlines/part*')
len(parts)

# COMMAND ----------

# MAGIC %fs head /databricks-datasets/airlines/part-00000

# COMMAND ----------

df = spark \
.read \
.option('header','True') \
.option('delimiter',',') \
.option('inferSchema','True') \
.csv('/databricks-datasets/airlines/part-00000')

# COMMAND ----------

df.printSchema()

# COMMAND ----------

display(df)

# COMMAND ----------

display(df.limit(5))

# COMMAND ----------

df.limit(5).show()

# COMMAND ----------

df = spark \
.read \
.option('header','True') \
.option('delimiter',',') \
.option('inferSchema','True') \
.csv('/databricks-datasets/airlines/part-00000')

# COMMAND ----------

schema = df.schema

# COMMAND ----------

df_partial = spark \
.read \
.option('header','True') \
.option('delimiter',',') \
.schema(schema) \
.csv('/databricks-datasets/airlines/part-0000*')

# COMMAND ----------

display(df.dtypes)

# COMMAND ----------

df.count()

# COMMAND ----------

display(df['year', 'month', 'dayofmonth', 'arrdelay','depdelay'])

# COMMAND ----------

from pyspark.sql.types import DoubleType

df = df.withColumn("ArrDelayDouble",df["ArrDelay"].cast(DoubleType()))

# COMMAND ----------

display(df.groupBy('Month').avg('arrdelaydouble'))

# COMMAND ----------

display(df
.select(['Year','ArrDelayDouble'])
.groupBy('Year')
.avg('ArrDelayDouble'))

# COMMAND ----------

display(df['Year','ArrDelayDouble']
.groupBy('Year')
.avg('ArrDelayDouble'))

# COMMAND ----------

display(df
.groupBy('Month')
.avg('ArrDelayDouble'))

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

display(df
.filter(df.Origin == 'SAN')
.groupBy('DayOfWeek')
.avg('arrdelaydouble'))

# COMMAND ----------

display(df.filter(df['Origin'] == 'SAN').groupBy('DayOfWeek').avg('arrdelaydouble'))

# COMMAND ----------

display(df
  .filter((df.Origin == 'SAN') & (df.Dest == 'SFO'))
  .groupBy('DayOfWeek')
  .avg('arrdelaydouble'))

# COMMAND ----------

display(df
        .filter(df.Origin != 'SAN')
        .filter(df.DayOfWeek < 3)
        .groupBy('DayOfWeek')
        .avg('arrdelaydouble'))

# COMMAND ----------

display(df
        .filter(df.Origin == 'SAN')
        .groupBy('DayOfWeek')
        .avg('arrdelaydouble')
        .sort('DayOfWeek'))

# COMMAND ----------

display(df
  .filter(df.Origin == 'SAN')
  .groupBy('DayOfWeek')
  .agg(round(mean('arrdelaydouble'),2).alias('AvgArrDelay'))
  .sort(desc('AvgArrDelay')))

# COMMAND ----------

display(df
        .filter(df.Origin == 'SAN')
        .groupBy('DayOfWeek')
        .agg(min('arrdelaydouble').alias('MinDelay')
             , max('arrdelaydouble').alias('MaxDelay')
             , (max('arrdelaydouble')-min('arrdelaydouble')).alias('Spread'))
)

# COMMAND ----------

df = df.withColumn('DayDate', to_date(concat('Year','Month','DayOfMonth'), 'yyyyMMdd')) 

# COMMAND ----------

display(df
  .select(date_format('DayDate', 'E').alias('WeekDay'), 'arrdelaydouble', 'origin', 'DayOfWeek')
  .filter(df.Origin == 'SAN')
  .groupBy('WeekDay','DayOfWeek')     
  .agg(round(mean('arrdelaydouble'),1).alias('AvgArrDelay'))
  .sort('DayOfWeek'))

# COMMAND ----------

start_date, end_date = df.select(min("DayDate"),date_add(min("DayDate"),30)).first()

print(start_date)
print(end_date)

# COMMAND ----------

display(df
        .filter(df.Origin == 'OAK')
        .filter(df
                .DayDate.between(start_date,end_date)
               )
        .groupBy('DayDate')
        .agg(mean('ArrDelay'))
        .orderBy('DayDate'))

# COMMAND ----------

display(df
        .filter(df.Origin.isin(['SFO','SAN','OAK']))
        .filter(df
                .DayDate.between(start_date,end_date)
               )
        .groupBy('Origin','DayDate')
        .agg(mean('ArrDelay'))
        .orderBy('DayDate'))

# COMMAND ----------

airport_list = ['SFO','SAN','OAK']

# COMMAND ----------

airport_list = [row.Origin for row in df.select('Origin').distinct().limit(5).collect()]

# COMMAND ----------

display(df
        .filter(df.Origin.isin(airport_list))
        .filter(df
                .DayDate.between(start_date,end_date)
               )
        .groupBy('Origin','DayDate')
        .agg(mean('ArrDelay'))
        .orderBy('DayDate'))

# COMMAND ----------

print(type(df.select('Origin').distinct()))
print(type(df.select('Origin').distinct().limit(5).collect()))
print(df.select('Origin').distinct().limit(5).collect())
print(df.select('Origin').distinct().limit(1).collect()[0].Origin)

# COMMAND ----------

airport_list = []
for row in df.select('Origin').distinct().limit(5).collect():
  airport_list.append(row.Origin)

# COMMAND ----------

df = df.withColumn('State', 
                    when(col('Origin') == 'SAN', 'California')
                    .when(df.Origin == 'LAX', 'California')
                    .when(df.Origin == 'SAN', 'California')
                    .when((df.Origin == 'JFK') | (df.Origin == 'LGA') | (df.Origin == 'BUF'), 'New York')
                    .otherwise('Other')
                   )

# COMMAND ----------

display(df.groupBy('State').count())

# COMMAND ----------

df = df.withColumn('State',when(col('Origin') == 'DFW', 'Texas').when(col('Origin') == 'IAH', 'Texas').when(col('Origin') == 'DAL','Texas').otherwise(col('State')))

# COMMAND ----------

display(df.groupBy('State').count())

# COMMAND ----------

df = df.withColumn('Flag', lit('Original'))

# COMMAND ----------

df_dfw = df.select('DayDate','Origin','Dest').filter(col('State') == 'California')

# COMMAND ----------

display(df_dfw.groupBy('Origin','Dest').count())

# COMMAND ----------

def bins(flights):
  if flights < 400:
    return 'Small'
  elif flights >= 1000:
    return 'Large'
  else: 
    return 'Medium'

# COMMAND ----------

df_temp = df.groupBy('Origin','Dest').agg(count('Origin').alias('count'), mean('arrdelaydouble').alias('AvgDelay'))

# COMMAND ----------

from pyspark.sql.types import StringType
bins_udf = udf(bins, StringType())
df_temp = df_temp.withColumn("Size", bins_udf("count"))

# COMMAND ----------

display(df_temp.groupBy('Size').agg(mean('AvgDelay')))

# COMMAND ----------

def bins(flights):
  ret = 'Small' if flights < 400 else ('Large' if flights >= 1000 else 'Medium')
  return ret

# COMMAND ----------

dfg = spark.createDataFrame([
  ['AA','American Airlines'], 
  ['DL', 'Delta Airlines'],
  ['UA', 'United Airlines']
], ['Shortname','Fullname'])


# COMMAND ----------

display(dfg)

# COMMAND ----------

df.createOrReplaceTempView('AirportData')
df_test = spark.sql('select * from AirportData')
display(df_test)

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from AirportData;

# COMMAND ----------

df1 = spark \
.read \
.option('header','True') \
.option('delimiter',',') \
.option('inferSchema','True') \
.csv('/databricks-datasets/airlines/part-00000')

df2 = spark \
.read \
.option('header','True') \
.option('delimiter',',') \
.option('inferSchema','True') \
.csv('/databricks-datasets/airlines/part-00001')

df_merged = df1.union(df2)

# COMMAND ----------

df_minus = df_merged.exceptAll(df2)

# COMMAND ----------

df1 = df.withColumn('MilesPerMinute', col("Distance") / col("ActualElapsedTime"))

# COMMAND ----------

df_airlines = spark.createDataFrame([
  ['AA','American Airlines'], 
  ['DL', 'Delta Airlines'],
  ['UA', 'United Airlines'],
  ['WN', 'Southwest Airlines']
], ['Shortname','Fullname'])

df_hq = spark.createDataFrame([
  ['AA','Fort Worth', 'Texas'], 
  ['DL', 'Chicago', 'Illinois'],
  ['UA', 'Atlanta', 'Georgia'],
  ['FR', 'Swords', 'Ireland'],
], ['Shortname','City', 'State'])

df_cities = spark.createDataFrame([
  ['San Fransisco'], 
  ['Miami'],
  ['Minneapolis']
], ['City'])


# COMMAND ----------

df_result = df_airlines.join(df_hq.withColumnRenamed('Shortname', 'SN'), col("Shortname") == col("SN"))

# COMMAND ----------

df_result = df_airlines.join(df_hq, ['Shortname'])

# COMMAND ----------

df_result = df_airlines.join(df_hq, ['Shortname'], 'left')

# COMMAND ----------

df_result = df_airlines.join(df_hq, ['Shortname'], 'right')

# COMMAND ----------

df_result = df_airlines.join(df_hq, ['Shortname'], 'outer')
