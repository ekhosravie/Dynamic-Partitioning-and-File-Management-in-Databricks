# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# COMMAND ----------

spark = SparkSession.builder.appName('DynamicPartitioning').getOrCreate()

# COMMAND ----------

data = [
    (1, "A", "2022-01-01"),
    (2, "B", "2022-01-02"),
    (3, "A", "2022-01-03"),
]

# COMMAND ----------

schema = ['id','category','date']

# COMMAND ----------

df = spark.createDataFrame(data,schema=schema)

# COMMAND ----------

parttion_columns = ['category','year_month']

# COMMAND ----------

df = df.withColumn('year_month',F.date_format('date','yyyy,MM'))

# COMMAND ----------

df.write.format('delta').mode('overwrite').partitionBy(*parttion_columns).save('/mnt/delta-table')

# COMMAND ----------

spark.read.format('delta').load('/mnt/delta-table').show()

# COMMAND ----------

  current_notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath()
print("Current Notebook Path:", current_notebook_path)

# COMMAND ----------


