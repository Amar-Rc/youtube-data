# Databricks notebook source
# MAGIC %md
# MAGIC #Process json data

# COMMAND ----------

# MAGIC %python
# MAGIC spark.conf.set("fs.azure.account.key.demotestbucket.dfs.core.windows.net", dbutils.secrets.get('testScope', 'test-demo-sk'))

# COMMAND ----------

# MAGIC %md
# MAGIC Read bronze files, selected only the items column and exploded it. Added a column called "category_region", as there were separate json files for each region. Created external silver tables on the json data.

# COMMAND ----------

for i in dbutils.fs.ls('abfss://bronze@demotestbucket.dfs.core.windows.net/raw_statistics_reference_data/'):
    df = spark.read.option('multiline','true').json(i.path)
    df.createOrReplaceTempView('df_v')
    spark.sql('select explode(items) as items from df_v').createOrReplaceTempView('df1_v')
    spark.sql(f'select distinct items.kind as kind, items.etag as etag, items.id as id, items.snippet.title as snippet_title, \
    items.snippet.channelId as snippet_channelId, \
items.snippet.assignable as snippet_assignable, "{i.name[:2].lower()}" as category_region from df1_v').write.format('delta').mode('append')\
    .option('overwriteSchema', 'true')\
    .option('path','abfss://silver@demotestbucket.dfs.core.windows.net/raw_statistics_reference_data/')\
    .saveAsTable('TEST_DEMO_SILVER.raw_statistics_reference_data')

# COMMAND ----------


