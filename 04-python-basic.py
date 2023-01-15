# Databricks notebook source
x = 1
if x == 1:
  print('One')
elif x == 2:
  print('Two')
else:
  print('Neither one or two')

# COMMAND ----------

for x in ['Spark','Hive','Hadoop']:
  print(x)

x = 0
while x < 10:
  print(x)
  x = x + 1

# COMMAND ----------

for i in range(1,10,1):
  print(i)

# COMMAND ----------

def my_function(input_var):
  return(input_var * 2)

my_function(4)

# COMMAND ----------

import time
time.sleep(10)

import time as t
t.sleep(10)

from time import sleep
sleep(10)

# COMMAND ----------

def my_function(input_var):
  return(input_var*2) 

# COMMAND ----------

s = '  spring'
s.replace('p','t').strip().capitalize()

# COMMAND ----------

try:
  1 + 1 + int('A')
except:
  print("You can't convert a letter to a number")
