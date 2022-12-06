# Databricks notebook source
# MAGIC %md
# MAGIC #Ingest raw data

# COMMAND ----------

# MAGIC %md
# MAGIC Connect to azure blob storage

# COMMAND ----------

spark.conf.set("fs.azure.account.key.demotestbucket.dfs.core.windows.net", dbutils.secrets.get('testScope', 'test-demo-sk'))

# COMMAND ----------

# MAGIC %md
# MAGIC Copy data to raw folder

# COMMAND ----------

dbutils.fs.cp("abfss://data@demotestbucket.dfs.core.windows.net/json", "abfss://bronze@demotestbucket.dfs.core.windows.net/raw_statistics_reference_data", recurse=True)
    

# COMMAND ----------

for i in dbutils.fs.ls("abfss://data@demotestbucket.dfs.core.windows.net/csv"):
    dbutils.fs.cp(i.path, f"abfss://bronze@demotestbucket.dfs.core.windows.net/raw_statistics/region={i.name[:2].lower()}/{i.name}")

# COMMAND ----------

# MAGIC %md
# MAGIC Create raw data tables

# COMMAND ----------

df_json = spark.read.option('multiline','true').json('abfss://bronze@demotestbucket.dfs.core.windows.net/raw_statistics_reference_data/')
df_json.createOrReplaceTempView('df_json_v')
spark.sql('create or replace table TEST_DEMO_BRONZE.raw_statistics_reference_data as select * from df_json_v')

# COMMAND ----------

df_csv = spark.read.csv("abfss://bronze@demotestbucket.dfs.core.windows.net/raw_statistics/", header = True, inferSchema = True)
df_csv.createOrReplaceTempView('df_csv_v')
spark.sql('create or replace table TEST_DEMO_BRONZE.raw_statistics using delta partitioned by (region) as select * from df_csv_v')



# COMMAND ----------


