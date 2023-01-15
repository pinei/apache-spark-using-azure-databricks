# Databricks notebook source
df = spark.createDataFrame(
 [
  ('Store 1',1,448),
  ('Store 1',2,None),
  ('Store 1',3,499),
  ('Store 1',44,432),
  (None,None,None),
  ('Store 2',1,355),
  ('Store 2',1,355),
  ('Store 2',None,345),
  ('Store 2',3,387),
  ('Store 2',4,312)
], 
 ['Store','WeekInMonth','Revenue'])

# COMMAND ----------

display(df.filter(df.Revenue.isNull()))

# COMMAND ----------

from pyspark.sql.functions import count, when, isnull
display(df.select(
  [count(when(isnull(c), c)).alias(c) for c in df.columns]
))

# COMMAND ----------

from pyspark.sql.functions import col
cols = [c for c in df.columns if df.filter(col(c).isNull()).count() > 0 ]

# COMMAND ----------

from functools import reduce

display(df.filter(reduce(lambda a1, a2: a1 | a2, (col(c).isNull() for c in cols))))

# COMMAND ----------

display(df.filter(reduce(lambda a1, a2: a1 | a2, (col(c).isNull() for c in df.columns))))

# COMMAND ----------

df2 = df.dropna()
display(df2)

# COMMAND ----------

df2 = df.dropna('all')

# COMMAND ----------

df2 = df.dropna(how='any', subset=['Store','WeekInMonth'])
display(df2)

# COMMAND ----------

display(df.dropna(thresh = 2))

# COMMAND ----------

display(df.dropna(thresh = 3))

# COMMAND ----------

display(df.dropna(thresh = 4))

# COMMAND ----------

display(df.fillna(0, ['Revenue']))

# COMMAND ----------

display(df.fillna('X'))

# COMMAND ----------

display(df.groupBy('Store').avg('Revenue'))

# COMMAND ----------

from pyspark.ml.feature import Imputer

df2 = df.withColumn('RevenueD', df.Revenue.cast('double'))

# COMMAND ----------

imputer = Imputer(
    inputCols=['RevenueD'], 
    outputCols=['RevenueD'],
    strategy='median')

display(imputer
        .fit(df2.filter(df2.Store == 'Store 1'))
        .transform(df2))

# COMMAND ----------

i = imputer.fit(df2.filter(df2.Store == 'Store 1'))
display(i.surrogateDF)

# COMMAND ----------

df2 = i.transform(df2)

# COMMAND ----------

display(df.dropDuplicates())

# COMMAND ----------

display(df.dropDuplicates(['Store']))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, desc

w =  Window.partitionBy(df['Store']).orderBy(df['WeekInMonth'].desc())
display(df
        .select(df['Store'], df['WeekInMonth'], 
                row_number().over(w).alias('Temp'))
        .filter(col('Temp') == 1))

# COMMAND ----------

display(df.select(df.Store, df.WeekInMonth, row_number()
 .over(w).alias('Temp'))
 .filter(col('Temp') == 1)
 .drop('Temp')) 

# COMMAND ----------

display(df.drop('Revenue'))

# COMMAND ----------

display(df.describe())

# COMMAND ----------

display(df.filter(df.Store == 'Store 1').describe())

# COMMAND ----------

from pyspark.sql.functions import mean, stddev 

display(df
        .groupBy('Store')
        .agg(mean('Revenue'), stddev('Revenue')))

# COMMAND ----------

df.approxQuantile('Revenue', [0.5], 0)

# COMMAND ----------

from pyspark.ml.feature import Bucketizer

mean_stddev = df \
  .filter(df.Store == 'Store 1') \
  .groupBy('Store') \
  .agg(mean('WeekInMonth').alias('M'), stddev('WeekInMonth').alias('SD')) \
  .select('M','SD') \
  .collect()[0] 

mean = mean_stddev['M']
stdev = mean_stddev['SD']

mini = max((mean - stdev),0)
maxi = mean + stdev

b = Bucketizer(splits = [ mini, mean, maxi, float('inf') ]
               ,inputCol = 'WeekInMonth'
               ,outputCol = "Bin")

dfb = b.transform(df
                    .select('WeekInMonth')
                    .filter(df['Store'] == 'Store 1'))
display(dfb)

# COMMAND ----------

display(dfb.groupBy('Bin').count())

# COMMAND ----------

df = spark.createDataFrame(
 [
  ('Robert',99)
], 
 ['Me, Myself & I','Problem %']
)

# COMMAND ----------

newnames = []
for c in df.columns:
  tmp = c.replace(',','-').replace('%','Pct').replace('&','And').replace(' ','')
  newnames.append(tmp)

display(df.toDF(*newnames))  

# COMMAND ----------

import re
newnames = []
for c in df.columns:
  tmp = re.sub('[^A-Za-z0-9]+', '', c)
  newnames.append(tmp)

display(df.toDF(*newnames))

# COMMAND ----------

df_pivoted = df.groupBy('WeekInMonth').pivot('Store').sum('Revenue').orderBy('WeekInMonth')

display(df_pivoted)

# COMMAND ----------

display(df.groupBy('Store','WeekInMonth').sum('Revenue').orderBy('WeekInMonth'))

# COMMAND ----------

display(df_pivoted
.withColumnRenamed('Store 1','Store1')
.withColumnRenamed('Store 2','Store2')
.selectExpr('WeekInMonth', 
"stack(2, 'Store 1', Store1, 'Store 2', Store2) as (Store, Revenue)"))

# COMMAND ----------

df = spark.sql("select stack(3,'Store 1',1, 448,'Store 1',2, 449,'Store 1',3, 450 ) as (Store,WeekInMonth,Revenue)")

display(df)

# COMMAND ----------

from pyspark.sql.functions import explode

df = spark.createDataFrame([
(1, ['Rolex','Patek','Jaeger']), 
(2, ['Omega','Heuer']), 
(3, ['Swatch','Rolex'])], 
('id','watches'))

display(df.withColumn('watches',explode(df.watches)))

# COMMAND ----------

df = spark.read.option('delimiter','\t').option('header','False').csv('/databricks-datasets/songs/data-001')

dftop25 = df.select('_c4').limit(25)
dftop25.repartition(4).rdd.glom().collect()
dftop25.count()


# COMMAND ----------

df = spark.read.option('delimiter',',').option('header','False').csv('/databricks-datasets/airlines/part-0001*')

print(df.rdd.getNumPartitions())
df.count()
