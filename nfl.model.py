# Databricks notebook source
# MAGIC %md
# MAGIC # Demo of querying tables and built in plotting
# MAGIC Lets look at the average points scored by team per game
# MAGIC 
# MAGIC ## NFL 2014 season playoffs

# COMMAND ----------

# MAGIC %pip install pyspark_dist_explore
# MAGIC %pip install mlflow

# COMMAND ----------

# trying to embbed image into markdown from within repo
# ![Playoffs](/Workspace/Repos/dev/databricks-nfl-data/img/nfl-playoffs-2014.png)

# COMMAND ----------

# Print the path
import sys
import os
# print("\n".join(sys.path))
# add path for repo
# sys.path.append(os.path.abspath('/Workspace/Repos/dev/databricks-nfl-data'))
# ls /Workspace/Repos/dev/databricks-nfl-data/img/nfl-playoffs-2014.png

# COMMAND ----------

# MAGIC %sql
# MAGIC -- the game stats fact table has a team id. need to get the team name from the dimension table
# MAGIC select 
# MAGIC   gs.points_scored
# MAGIC   , gs.team_id
# MAGIC   , tl.division
# MAGIC   , tl.team_long
# MAGIC from
# MAGIC   nfl.game_stats gs
# MAGIC   inner join nfl.team_lookup tl
# MAGIC on gs.team_id=tl.team_id

# COMMAND ----------

# MAGIC %md
# MAGIC # Test basic end to end model prediction workflow
# MAGIC 
# MAGIC Using this as an example [Descision Tree](https://docs.databricks.com/_static/notebooks/decision-trees-sfo-airport-survey-example.html)
# MAGIC 
# MAGIC Needs more work. At least it runs end to end.

# COMMAND ----------

from pyspark.sql import functions as F
# Read the Delta Lake table from ADLS
game_stats = spark.sql("""
select
  *
from
  nfl.game_stats
;
""")
# code the string Win Loss column to binary for target
df = game_stats.withColumn('WIN_LOSS_BINARY',F.when((F.col("WIN_LOSS") == 'W'), 1).when((F.col("WIN_LOSS") == 'L') | (F.col("WIN_LOSS") == 'T'), 0).otherwise(0))

# COMMAND ----------

# Display sample records
display(df)

# COMMAND ----------

## Only select some columns
df = df.select("TOTAL_FIRST_DOWNS", "PASSING_FIRST_DOWNS","YARDS_PER_PLAY","FUMBLES","TOTAL_YARDS","INTERCEPTIONS","PUNTS","WIN_LOSS_BINARY")

# COMMAND ----------

# MAGIC %md
# MAGIC # Histograms of columns
# MAGIC 
# MAGIC Note there is a difference between a Spark dataframe and a pandas dataframe. They are not the same and do not work in the same way with the same functions/methods.  
# MAGIC If you want to leverage the parallel nature of Spark, and you want to leverage Delta Lake on ADLS you need to learn how to harness these technologies.

# COMMAND ----------

## to plot stuff using Spark needed to install pyspark_dist_explore
## added the pip install the packageInstall_zakaria init script as I still haven't figured out how to get to the internet from a cluster with process isolation
from pyspark_dist_explore import hist
import matplotlib.pyplot as plt

## create a figure with 4 subplots
fig, ax = plt.subplots(nrows=2, ncols=2)
fig.set_size_inches(20,20)

## top left corner
hist(ax[0,0], [df.select('TOTAL_FIRST_DOWNS'), df.select('PASSING_FIRST_DOWNS')], bins = 15, color=['red', 'green'])
ax[0,0].set_title('comparison')
ax[0,0].legend()

## top right corner
hist(ax[0,1], [df.select('YARDS_PER_PLAY'), df.select('FUMBLES')], bins = 6, color=['blue', 'orange'])
ax[0,1].set_title('comparison')
ax[0,1].legend()

## bottom left corner (alpha is opacity, why, I don't know? couldn't the argument be called opacity?)
hist(ax[1,0], [df.select('TOTAL_YARDS')], bins = 15, color=['purple'], alpha=0.1)
ax[1,0].set_title('comparison')
ax[1,0].legend()

## bottom right corner
hist(ax[1,1], [df.select('INTERCEPTIONS'), df.select('PUNTS')], bins = 8, color=['purple', 'pink'])
ax[1,1].set_title('comparison')
ax[1,1].legend()

# COMMAND ----------

# MAGIC %md
# MAGIC The machine learning pipeline tracked and registered using MLFlow

# COMMAND ----------

import mlflow
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import DecisionTreeRegressor
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.ml.evaluation import RegressionEvaluator

mlflow.sklearn.autolog()

def trainModel():
  '''
    Train a Descision tree model
    Return:
      run_id: an UUID representing the MLFLow model run
  '''
  with mlflow.start_run():
    # The input features
    inputCols = ["TOTAL_FIRST_DOWNS", "PASSING_FIRST_DOWNS","YARDS_PER_PLAY","FUMBLES","TOTAL_YARDS","INTERCEPTIONS","PUNTS"]
    va = VectorAssembler(inputCols=inputCols,outputCol="features")
    # Define the Decision tree regressor
    dt = DecisionTreeRegressor(labelCol="WIN_LOSS_BINARY", featuresCol="features", maxDepth=4)
    evaluator = RegressionEvaluator(metricName = "rmse", labelCol="WIN_LOSS_BINARY")

    grid = ParamGridBuilder().addGrid(dt.maxDepth, [3, 5, 7, 10]).build()
    cv = CrossValidator(estimator=dt, estimatorParamMaps=grid, evaluator=evaluator, numFolds = 10)
    pipeline = Pipeline(stages=[va, dt])
    
    # Now call fit to train the model and then use MLFlow to log the model
    model = pipeline.fit(df)
    mlflow.spark.log_model(spark_model=model, artifact_path="model")
    
    # Log the RMSE to MLFlow
    predictions = model.transform(df)
    rmse = evaluator.evaluate(predictions, {evaluator.metricName: "rmse"})
    mlflow.log_metric("rmse", rmse) 
    
    # Return the run
    run_id = mlflow.active_run().info.run_id
    return run_id
  
run_id = trainModel()


# COMMAND ----------

# Register the model
model_name = "nfl-decisiontree"
artifact_path = "model"
model_uri = f"runs:/{run_id}/{artifact_path}"
 
model_details = mlflow.register_model(model_uri=model_uri, name=model_name)

# COMMAND ----------

# Get the model from the registry
model = mlflow.spark.load_model(
    model_uri=f"models:/nfl-decisiontree/{model_details.version}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC Now call `display` to view the tree.

# COMMAND ----------

display(model.stages[-1])
df.schema.fields

# COMMAND ----------

# MAGIC %md
# MAGIC Call `transform` to view the predictions.

# COMMAND ----------

df

# COMMAND ----------

predictions = model.transform(df)
display(predictions)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Evaluate the model
# MAGIC Validate the model using root mean squared error (RMSE).

# COMMAND ----------

# MAGIC %md
# MAGIC Save the model to a file in ADLS if you want

# COMMAND ----------

import uuid
model_save_path = f"abfss://nfldata@nfl.dfs.core.windows.net/models/{str(uuid.uuid4())}"
model.write().overwrite().save(model_save_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Feature Importance
# MAGIC Now let's look at the feature importances. The variable is shown below.

# COMMAND ----------

model.stages[1].featureImportances

# COMMAND ----------

# MAGIC %md
# MAGIC Feature importance is a measure of information gain. It is scaled from 0.0 to 1.0. As an example, feature 1 in the example above is rated as 0.0775 or 7.75% of the total importance for all the features.
# MAGIC Let's map the features to their proper names to make them easier to read.

# COMMAND ----------

featureNames = map(lambda s: s.name, df.schema.fields)
fieldMap = zip(df.schema.fields, featureNames)
featureImportance = model.stages[1].featureImportances.toArray()
featureImportanceMap = zip(featureImportance, featureNames)
# Let's convert this to a DataFrame so you can view it and save it so other users can rely on this information.
importancesDf = spark.createDataFrame(sc.parallelize(featureImportanceMap).map(lambda r: [r[1], float(r[0])]))
importancesDf = importancesDf.withColumnRenamed("_1", "Feature").withColumnRenamed("_2", "Importance")
display(importancesDf.sort("Importance"))

# COMMAND ----------

# MAGIC %md
# MAGIC As you can see the 3 most important features are:
# MAGIC 
# MAGIC 1. Interceptions
# MAGIC 1. Total Yards
# MAGIC 1. Fumbles
# MAGIC 
# MAGIC This is useful information for the NFL teams. 
# MAGIC It means that avoiding passing interceptions and fumbling as well as gaining lots of yards is the most important thing to winning.
