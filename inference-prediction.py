import sys
import numpy as np
import pandas as pd
import ast
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, size, pandas_udf
from pyspark.sql.types import ArrayType, DoubleType
from pyspark.ml import PipelineModel

@pandas_udf(ArrayType(DoubleType()))
def process_coordinates_pandas(geo_type_series, coords_series):
    results = []
    for geo_type, coords in zip(geo_type_series, coords_series):
        if geo_type == "Point" and len(coords) == 2:
            try:
                results.append([float(coords[0]), float(coords[1])])
            except ValueError:
                results.append(None)
        elif geo_type == "LineString":
            try:
                parsed_coords = [ast.literal_eval(c) for c in coords if isinstance(c, str)]
                valid_coords = [c for c in parsed_coords if len(c) == 2]
                lats = [float(c[1]) for c in valid_coords]
                lons = [float(c[0]) for c in valid_coords]
                if lats and lons:
                    results.append([float(np.mean(lats)), float(np.mean(lons))])
                else:
                    results.append(None)
            except Exception:
                results.append(None)
        else:
            results.append(None)
    return pd.Series(results)

def main(input_json_path, model_path, output_path):
    spark = SparkSession.builder.appName("Predict-Duration-With-Pipeline").getOrCreate()

    raw_df = spark.read.option("multiline", "true").json(input_json_path)

    df = raw_df.dropDuplicates(["id"]) \
                  .dropna(subset=["id", "geography", "event_type"]) \
                  .withColumn("created", to_timestamp(col("created"))) \
                  .withColumn("updated", to_timestamp(col("updated"))) \
                  .withColumn("coordinates", process_coordinates_pandas(col("geography.type"), col("geography.coordinates"))) \
                  .withColumn("latitude", col("coordinates")[0]) \
                  .withColumn("longitude", col("coordinates")[1]) \
                  .dropna(subset=["latitude", "longitude"]) \
                  .withColumn("num_roads", size(col("roads"))) \
                  .withColumn("num_areas", size(col("areas"))) \
                  .drop("geography", "coordinates", "roads", "areas", "headline", "description", "schedule", "+ivr_message", "url", "jurisdiction_url")


    pipeline_model = PipelineModel.load(model_path)
    predictions = pipeline_model.transform(df)

    results = predictions.select("id", "prediction")

    results.write.mode('overwrite').parquet(output_path)

    print("Predictions written to predictions.txt")
    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: spark-submit predict_duration_pipeline.py <input_json> <model_path> <predictions_output_path>")
        sys.exit(1)

    main(sys.argv[1], sys.argv[2], sys.argv[3])