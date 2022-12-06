-- Databricks notebook source
-- MAGIC %md
-- MAGIC #Analyze data

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.conf.set("fs.azure.account.key.demotestbucket.dfs.core.windows.net", dbutils.secrets.get('testScope', 'test-demo-sk'))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Created gold table by join the the raw statistics and raw_statistics_reference_data

-- COMMAND ----------

use test_demo_silver;
create or replace table test_demo_gold.final_analytics partitioned by (region, category_id) 
location "abfss://gold@demotestbucket.dfs.core.windows.net/final_analytics/"
as
select * from raw_statistics rs, raw_statistics_reference_data rsrd
where rs.category_id = rsrd.id and rs.region = rsrd.category_region;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Generated visualizations and dashboard on the gold data

-- COMMAND ----------

select region, snippet_title, sum(views) from test_demo_gold.final_analytics group by snippet_title, region order by sum(views) desc

-- COMMAND ----------

select region, snippet_title, sum(likes) from test_demo_gold.final_analytics group by snippet_title, region order by sum(likes) desc

-- COMMAND ----------

select region, snippet_title, sum(dislikes) from test_demo_gold.final_analytics group by snippet_title, region order by sum(dislikes) desc

-- COMMAND ----------

select region, snippet_title, sum(comment_count) from test_demo_gold.final_analytics group by snippet_title, region order by sum(comment_count) desc

-- COMMAND ----------


