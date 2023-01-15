# Databricks notebook source
# MAGIC %sh 
# MAGIC mkdir /tmp
# MAGIC cd /tmp
# MAGIC wget http://ergast.com/downloads/f1db_csv.zip

# COMMAND ----------

# MAGIC %sh ls /tmp

# COMMAND ----------

# MAGIC %sh
# MAGIC unzip -Z1 /tmp/f1db_csv.zip

# COMMAND ----------

# MAGIC %sh
# MAGIC unzip -j /tmp/f1db_csv.zip constructors.csv -d /tmp

# COMMAND ----------

# MAGIC %sh ls /tmp/*.csv

# COMMAND ----------

# MAGIC %sh
# MAGIC mkdir /dbfs/tmp
# MAGIC cp /tmp/constructors.csv /dbfs/tmp/constructors.csv

# COMMAND ----------

from requests import get
with open('/tmp/f1.zip','wb') as file:
  response = get('http://ergast.com/downloads/f1db_csv.zip')
  file.write(response.content)

# COMMAND ----------

from zipfile import ZipFile
with ZipFile('/tmp/f1.zip','r') as zip:
  files = zip.namelist()
  for file in files:
    print(file)

# COMMAND ----------

with ZipFile('/tmp/f1.zip','r') as zip:
  zip.extract('seasons.csv','/tmp')

# COMMAND ----------

import os
os.listdir('/tmp')

# COMMAND ----------

dbutils.fs.mv('file:/tmp/seasons.csv','dbfs:/tmp/seasons.csv')

# COMMAND ----------

df = spark.read.format('csv').option('inferSchema','true').option('header','false').load('/tmp/seasons.csv').selectExpr('_c0 as year', '_c1 as url')

# COMMAND ----------

df.write.saveAsTable('seasons')

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from seasons;
