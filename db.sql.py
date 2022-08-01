# Databricks notebook source
# MAGIC %md
# MAGIC # Creating the tables
# MAGIC Assuming you have done the previous steps outlined in the blog/README and you have an ADLS Gen2 storage account as well as a container with the CSV files loaded you can continue from this Databricks notebook.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- create a Hive metastore database to register the tables
# MAGIC create database nfl

# COMMAND ----------

# A function to create a delta table
def deltaFromFile(file_location: str, file_type: str = "csv", infer_schema: str = "true", first_row_is_header: str = "true"):
  '''
    Create a delta table and register in Hive metastore from a CSV
    Args:
      file_location: the full path location within ADLS
      file_type: the extension (type) of the file
      infer_schema: boolean string if you want to infer the schema
      first_row_is_header: boolean string if the first row contains column names
    Returns:
      None
  '''
  # file_name 
  file_name = file_location.split('/')[-1].split('.')[0]
  # delimiter option
  delimiter = "," if file_type == "csv" else " "
  # The applied options are for CSV files. For other file types, these will be ignored.
  df = spark.read.format(file_type) \
    .option("inferSchema", infer_schema) \
    .option("header", first_row_is_header) \
    .option("sep", delimiter) \
    .load(file_location)
  # create a temp view
  df.createOrReplaceTempView("temp")
  # write the parquet delta table
  df.write.mode("overwrite") \
    .format("delta") \
    .saveAsTable(f"nfl.{file_name}")

# COMMAND ----------

# A function to loop through all files in an ADLS container and create delta tables
def deltaForAllFiles(adls_path: str = 'abfss://nfldata@nfl.dfs.core.windows.net/'):
  '''
    Creates a delta table for all files in an ADLS container
    Args:
      adls_path: the path to an ADLS container or folder containing files
    Returns:
      None
  '''
  all_files = dbutils.fs.ls(adls_path)
  for file in all_files:
    print(file.path)
    deltaFromFile(file.path)

# COMMAND ----------

# run it all
deltaForAllFiles()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- test a query on the resulting tables
# MAGIC select
# MAGIC   p.PLAYER_NAME
# MAGIC   , p.FIELD_POSITION
# MAGIC   , t.TEAM_SHORT
# MAGIC   , p.RUSHING_YARDS
# MAGIC   , p.PASSING_YARDS
# MAGIC from
# MAGIC   nfl.player p
# MAGIC   inner join nfl.team_lookup t
# MAGIC on p.team_id = t.team_id
# MAGIC   and t.team_short = 'SF'
# MAGIC order by p.RUSHING_YARDS desc

# COMMAND ----------


