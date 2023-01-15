# Databricks notebook source
# MAGIC %md
# MAGIC MLlib

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists products;
# MAGIC 
# MAGIC create table products 
# MAGIC (product_id integer, product_name string);
# MAGIC 
# MAGIC insert into products values (1, 'Coca Cola');
# MAGIC insert into products values (2, 'Pepsi Cola');
# MAGIC insert into products values (3, 'RedBull');
# MAGIC insert into products values (4, 'Evian');

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists cart;
# MAGIC 
# MAGIC create table cart (cart_id integer, product_id integer);
# MAGIC 
# MAGIC insert into cart values (1, 1);
# MAGIC insert into cart values (1, 2);
# MAGIC insert into cart values (1, 3);
# MAGIC insert into cart values (2, 1);
# MAGIC insert into cart values (3, 2);
# MAGIC insert into cart values (3, 3);
# MAGIC insert into cart values (3, 4);
# MAGIC insert into cart values (4, 2);
# MAGIC insert into cart values (4, 3);
# MAGIC insert into cart values (5, 1);
# MAGIC insert into cart values (5, 3);
# MAGIC insert into cart values (6, 1);
# MAGIC insert into cart values (6, 3);
# MAGIC insert into cart values (7, 2);
# MAGIC insert into cart values (8, 4);
# MAGIC insert into cart values (9, 1);
# MAGIC insert into cart values (9, 3);
# MAGIC insert into cart values (10, 1);
# MAGIC insert into cart values (10, 3);
# MAGIC insert into cart values (10, 4);

# COMMAND ----------

from pyspark.sql import Row

rdd = sc.parallelize([Row(cart_id=1,products=['RedBull','Coca Cola','Pepsi Cola']), Row(cart_id=2,products=['Coca Cola'])])

df = rdd.toDF()
display(df)

# COMMAND ----------

carts = spark.sql('select p.product_name, c.cart_id from products p join cart c on (c.product_id = p.product_id)')
display(carts)

from pyspark.sql.functions import collect_set

preppedcarts = carts.groupBy('cart_id').agg(collect_set('product_name').alias('products'))

display(preppedcarts)

# COMMAND ----------

from pyspark.ml.fpm import FPGrowth

fpGrowth = FPGrowth(itemsCol="products", minSupport=0.5, minConfidence=0.5)

model = fpGrowth.fit(preppedcarts)

# COMMAND ----------

display(model.freqItemsets)

# COMMAND ----------

display(model.associationRules)

# COMMAND ----------

display(model.transform(preppedcarts))

# COMMAND ----------

# MAGIC %md
# MAGIC MLflow

# COMMAND ----------

dbutils.library.installPyPI('mlflow')
dbutils.library.installPyPI('scikit-learn')
dbutils.library.restartPython()

# COMMAND ----------

import pandas as pd
import sklearn
from sklearn.datasets import load_boston

basedata = load_boston()
pddf = pd.DataFrame(basedata.data
,columns=basedata.feature_names)
pddf['target'] = pd.Series(basedata.target)

pct20 = int(pddf.shape[0]*.2)

testdata = spark.createDataFrame(pddf[:pct20])
traindata = spark.createDataFrame(pddf[pct20:])

# COMMAND ----------

from pyspark.ml.feature import VectorAssembler

va = VectorAssembler(inputCols = basedata.feature_names,outputCol = 'features')

testdata = va.transform(testdata)['features','target']
traindata = va.transform(traindata)['features','target']

# COMMAND ----------

from pyspark.ml.regression import LinearRegression
import mlflow

for i in range(1,4):
  with mlflow.start_run():
    mi = 10 * i
    rp = 0.1 * i
    enp = 0.5

    mlflow.log_param('maxIter',mi)
    mlflow.log_param('regParam',rp)
    mlflow.log_param('elasticNetParam',enp)

    lr = LinearRegression(maxIter=mi,regParam=rp,elasticNetParam=enp,labelCol="target")

    model = lr.fit(traindata)
    pred = model.transform(testdata)

    r = pred.stat.corr("prediction", "target")
    mlflow.log_metric("rsquared", r**2, step=i)


# COMMAND ----------

# MAGIC %md
# MAGIC Koalas

# COMMAND ----------

import pandas as pd

models = {
  'Model' : ['Model S'
,'Model 3'
,'Model Y'
,'Model X'
,'Cybertruck'],
  'Entry Price' : [85000, 35000, 45000, 89500, 50000]
}

df = pd.DataFrame(models, columns = ['Model', 'Entry Price'])
df.columns = ['Model','Price']

print(df)
print(type(df))

# COMMAND ----------

dbutils.library.installPyPI("koalas")
dbutils.library.restartPython()

# COMMAND ----------

import databricks.koalas as ks

models = {
  'Model' : ['Model S'
,'Model 3'
,'Model Y'
,'Model X'
,'Cybertruck'],
  'Entry Price' : [85000, 35000, 45000, 89500, 50000]
         }
df = ks.DataFrame(models, columns = ['Model', 'Entry Price'])
df.columns = ['Model','Price']

print(df)
print(type(df))

# COMMAND ----------

import pandas as pd

df0 = pd.read_csv('/dbfs/databricks-datasets/airlines/part-00000')  
df1 = pd.read_csv('/dbfs/databricks-datasets/airlines/part-00001',header=0)
df1 = pd.DataFrame(data=df1.values, columns=df0.columns)

df = pd.concat([df0,df1], ignore_index=True)
df[['Year','Month']].groupby('Year').sum()

# COMMAND ----------

df0 = ks.read_csv('/databricks-datasets/airlines/part-00000')  
df1 = ks.read_csv('/databricks-datasets/airlines/part-00001',header=0)

df1.columns = df0.columns
df = ks.concat([df0,df1], ignore_index=True)

df[['Year','Month']].groupby('Year').sum()

# COMMAND ----------

# MAGIC %md
# MAGIC Data presentation

# COMMAND ----------

df = spark.read.format("csv").option("inferSchema", "true").option("header", "true").load("/databricks-datasets/airlines/part-00000")

# COMMAND ----------

from pyspark.sql.functions import count

display(df
.select('Origin','DayOfWeek')
.filter(df.Origin.isin('LAS','SAN','OAK'))
.groupBy('Origin','DayOfWeek')
.agg(count('Origin').alias('NumberOfFlights'))
.orderBy('DayOfWeek'))

# COMMAND ----------

from pyspark.sql.functions import count

display(df
.select('UniqueCarrier','DayOfMonth')
.filter(df.UniqueCarrier.isin('UA','PI'))
.groupBy('UniqueCarrier','DayOfMonth')
.count()
.orderBy('DayOfMonth'))

# COMMAND ----------

df2pd = df.withColumn('arrdelayint', df.ArrDelay.cast("int"))
df2pd = df2pd.select('DayOfWeek','arrdelayint').filter(df2pd.arrdelayint.between(-20,20)).na.drop()

# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt

pddf = df2pd.toPandas()
fig, ax = plt.subplots()

pddf.boxplot(column=['arrdelayint'], by='DayOfWeek', ax=ax)

plt.suptitle('Boxplots')
ax.set_title('axes title')
ax.set_xlabel('Day of week')
ax.set_ylabel('Delay')

display()

# COMMAND ----------

# MAGIC %md
# MAGIC Streaming

# COMMAND ----------

dbutils.fs.mkdirs('/streamtst')

# COMMAND ----------

import json
import random
import time

d = {'unix_timestamp':time.time(),'random_number':random.randint(1,10)}

with open('/dbfs/streamtst/0.json', 'w') as f:
    json.dump(d, f)

# COMMAND ----------

# MAGIC %fs ls /streamtst/

# COMMAND ----------

from pyspark.sql.types import TimestampType, IntegerType, StructType, StructField

schema = StructType([StructField("unix_timestamp", TimestampType(), True), StructField("random_number", IntegerType(), True) ])

# COMMAND ----------

dfin = (spark.readStream.schema(schema).option('maxFilesPerTrigger',1).json('/streamtst/'))

# COMMAND ----------

dfstream = (dfin.groupBy(dfin.random_number).count())

# COMMAND ----------

dfrun = (
  dfstream
    .writeStream
    .format("memory")
    .queryName("random_numbers")
    .outputMode("complete")
    .start()
)

# COMMAND ----------

# MAGIC %sql select * from random_numbers;

# COMMAND ----------

d = {'unix_timestamp':time.time(),'random_number':random.randint(1,10)}

with open('/dbfs/streamtst/1.json', 'w') as f:
    json.dump(d, f)

# COMMAND ----------

# MAGIC %sql select * from random_numbers;

# COMMAND ----------

for i in range(2,100):
  d = {'unix_timestamp':time.time(),'random_number':random.randint(1,10)}
  
  with open('/dbfs/streamtst/{}.json'.format(i), 'w') as f:
      json.dump(d, f)

# COMMAND ----------

# MAGIC %fs ls /streamtst/

# COMMAND ----------

for stream in spark.streams.active:
  print("{}, {}".format(stream.name, stream.id))

# COMMAND ----------

for stream in spark.streams.active:
  stream.stop()

# COMMAND ----------

# MAGIC %sql select * from random_numbers;
