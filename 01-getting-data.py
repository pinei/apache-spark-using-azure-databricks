# Databricks notebook source
# MAGIC %md
# MAGIC ### Databricks datasets
# MAGIC 
# MAGIC https://docs.databricks.com/dbfs/databricks-datasets.html#databricks-datasets-databricks-datasets

# COMMAND ----------

# MAGIC %fs ls /databricks-datasets/

# COMMAND ----------

# MAGIC %fs ls /databricks-datasets/airlines/

# COMMAND ----------

# MAGIC %fs head /databricks-datasets/airlines/README.md

# COMMAND ----------

# MAGIC %fs cp /databricks-datasets/airlines/README.md /

# COMMAND ----------

# MAGIC %fs ls /README.md

# COMMAND ----------

# MAGIC %fs rm /README.md

# COMMAND ----------

dbutils.fs.ls('/databricks-datasets')

# COMMAND ----------

dbutils.fs.head('/databricks-datasets/airlines/README.md')

# COMMAND ----------

for f in dbutils.fs.ls('/databricks-datasets/'):
  print(f.name)

# COMMAND ----------

displayHTML("<img src = 'https://upload.wikimedia.org/wikipedia/commons/e/eb/Apress-logo.png'>")

# COMMAND ----------

# MAGIC %sh 
# MAGIC mkdir /dbfs/FileStore/images
# MAGIC cd /dbfs/FileStore/images
# MAGIC wget https://upload.wikimedia.org/wikipedia/commons/e/eb/Apress-logo.png

# COMMAND ----------

# MAGIC %fs ls /FileStore/images

# COMMAND ----------

displayHTML("<img src = '/files/images/Apress-logo.png'>")
