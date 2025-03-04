import sys
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, size
from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer
from pyspark.ml.regression import RandomForestRegressor, GBTRegressor
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.ml.evaluation import RegressionEvaluator


parquet_folder_path = sys.argv[1]

spark = SparkSession.builder.appName("ML-Duration-Prediction").getOrCreate()

df_spark = spark.read.parquet(parquet_folder_path)

df_spark = df_spark.withColumn("created", to_timestamp(col("created")))
df_spark = df_spark.withColumn("updated", to_timestamp(col("updated")))

df_spark = df_spark.withColumn("duration", (col("updated").cast("long") - col("created").cast("long")) / (60 * 60 * 24))

df_spark = df_spark.dropna(subset=["duration"])

df_spark = df_spark.withColumn("num_roads", size(col("roads")))
df_spark = df_spark.withColumn("num_areas", size(col("areas")))

df_spark = df_spark.withColumn("latitude", col("latitude"))  
df_spark = df_spark.withColumn("longitude", col("longitude"))

drop_columns = ["jurisdiction_url", "url", "id", "headline", "description", "+ivr_message", "roads", "areas", "schedule"]
df_spark = df_spark.drop(*drop_columns)

categorical_columns = ["event_type", "severity", "status"]
indexers = [StringIndexer(inputCol=col, outputCol=col+"_index", handleInvalid="keep") for col in categorical_columns]

feature_cols = ["num_roads", "num_areas", "latitude", "longitude", "event_type_index", "severity_index", "status_index"]
vector_assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

scaler = StandardScaler(inputCol="features", outputCol="scaled_features", withStd=True, withMean=True)

for indexer in indexers:
    df_spark = indexer.fit(df_spark).transform(df_spark)

df_spark = vector_assembler.transform(df_spark)
df_spark = scaler.fit(df_spark).transform(df_spark)

train, test = df_spark.randomSplit([0.8, 0.2], seed=42)

rf = RandomForestRegressor(featuresCol="scaled_features", labelCol="duration", numTrees=100, maxDepth=5, maxBins=32)
gbt = GBTRegressor(featuresCol="scaled_features", labelCol="duration", maxIter=100, maxDepth=5)

evaluator = RegressionEvaluator(labelCol="duration", predictionCol="prediction", metricName="rmse")

param_grid = ParamGridBuilder() \
    .addGrid(rf.numTrees, [50, 100, 150]) \
    .addGrid(rf.maxDepth, [5, 10, 15]) \
    .addGrid(rf.maxBins, [16, 32]) \
    .build()

crossval = CrossValidator(estimator=rf, estimatorParamMaps=param_grid, evaluator=evaluator, numFolds=3)

cv_model = crossval.fit(train)

best_rf = cv_model.bestModel
best_rf_predictions = best_rf.transform(test)
rf_rmse = evaluator.evaluate(best_rf_predictions)

gbt_model = gbt.fit(train)
gbt_predictions = gbt_model.transform(test)
gbt_rmse = evaluator.evaluate(gbt_predictions)

if gbt_rmse < rf_rmse:
    best_model = gbt_model
    best_rmse = gbt_rmse
else:
    best_model = best_rf
    best_rmse = rf_rmse

print(f"Best Model RMSE: {best_rmse}")

spark.stop()
