-- Databricks notebook source
-- MAGIC %md
-- MAGIC #Process csv files

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.conf.set("fs.azure.account.key.demotestbucket.dfs.core.windows.net", dbutils.secrets.get('testScope', 'test-demo-sk'))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Created external table on the bronze raw statistics table

-- COMMAND ----------

CREATE OR REPLACE TABLE TEST_DEMO_SILVER.raw_statistics partitioned by (region) location "abfss://silver@demotestbucket.dfs.core.windows.net/raw_statistics/" as select * from test_demo_bronze.raw_statistics;


-- COMMAND ----------


