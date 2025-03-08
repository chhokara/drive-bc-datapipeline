import sys
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, size
from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer, OneHotEncoder
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.types import FloatType
import tensorflow as tf
from tensorflow import keras
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense, Dropout
from tensorflow.keras.optimizers import Adam
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

if len(sys.argv) < 2:
    print("Usage: spark-submit ml_nn.py <parquet_folder_path>")
    sys.exit(1)

parquet_folder_path = sys.argv[1]

spark = SparkSession.builder.appName("MLP-Duration-Prediction").getOrCreate()

df_spark = spark.read.parquet(parquet_folder_path)

df_spark = df_spark.withColumn("created", to_timestamp(col("created")))
df_spark = df_spark.withColumn("updated", to_timestamp(col("updated")))
df_spark = df_spark.withColumn("duration", ((col("updated").cast("long") - col("created").cast("long")) / (60 * 60 * 24)).cast(FloatType()))
df_spark = df_spark.dropna(subset=["duration"])

df_spark = df_spark.withColumn("num_roads", size(col("roads")))
df_spark = df_spark.withColumn("num_areas", size(col("areas")))
df_spark = df_spark.withColumn("latitude", col("latitude"))
df_spark = df_spark.withColumn("longitude", col("longitude"))

drop_columns = ["jurisdiction_url", "url", "id", "headline", "description", "+ivr_message", "roads", "areas", "schedule"]
df_spark = df_spark.drop(*drop_columns)

categorical_columns = ["event_type", "severity", "status"]
indexers = [StringIndexer(inputCol=col, outputCol=col+"_index", handleInvalid="keep") for col in categorical_columns]
one_hot_encoders = [OneHotEncoder(inputCol=indexer.getOutputCol(), outputCol=indexer.getOutputCol() + "_onehot") for indexer in indexers]

feature_cols = ["num_roads", "num_areas", "latitude", "longitude"] + [enc.getOutputCol() for enc in one_hot_encoders]
vector_assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
scaler = StandardScaler(inputCol="features", outputCol="scaled_features", withStd=True, withMean=True)

for indexer, encoder in zip(indexers, one_hot_encoders):
    df_spark = indexer.fit(df_spark).transform(df_spark)
    df_spark = encoder.fit(df_spark).transform(df_spark)

df_spark = vector_assembler.transform(df_spark)
df_spark = scaler.fit(df_spark).transform(df_spark)

df_pandas = df_spark.select("scaled_features", "duration").toPandas()
X = np.array(df_pandas["scaled_features"].tolist())
y = df_pandas["duration"].values

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

model = Sequential([
    Dense(128, activation='relu', input_shape=(X_train.shape[1],)),
    Dropout(0.2),
    Dense(64, activation='relu'),
    Dropout(0.2),
    Dense(32, activation='relu'),
    Dense(1)
])

model.compile(optimizer=Adam(learning_rate=0.001), loss='mse', metrics=['mae'])
history = model.fit(X_train, y_train, validation_data=(X_test, y_test), epochs=100, batch_size=32, verbose=1)

y_pred = model.predict(X_test)

rmse = np.sqrt(mean_squared_error(y_test, y_pred))
mae = mean_absolute_error(y_test, y_pred)
mse = mean_squared_error(y_test, y_pred)
r2 = r2_score(y_test, y_pred)

results_text = f"""
Neural Network Regression Model - Duration Prediction

RMSE: {rmse}
MAE: {mae}
MSE: {mse}
R² Score: {r2}
"""

with open("mlp_results.txt", "w") as f:
    f.write(results_text)

print(results_text)

spark.stop()
