import numpy as np
import pandas as pd
import ast
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, explode, pandas_udf
from pyspark.sql.types import ArrayType, DoubleType
import sys
assert sys.version_info >= (3, 5)


def read_data(spark, input_path_events):
    raw_events = spark.read.option("multiline", "true").json(input_path_events)

    # Print schema to confirm structure
    raw_events.printSchema()

    return raw_events



@pandas_udf(ArrayType(DoubleType()))
def process_coordinates_pandas(geo_type_series, coords_series):
    results = []
    for geo_type, coords in zip(geo_type_series, coords_series):
        if geo_type == "Point":
            if len(coords) == 2:
                try:
                    results.append([float(coords[0]), float(coords[1])])
                except ValueError:
                    results.append(None)
            else:
                results.append(None)
        elif geo_type == "LineString":
            if len(coords) > 0:
                try:
                    parsed_coords = [ast.literal_eval(
                        coord) for coord in coords]

                    valid_coords = [
                        coord for coord in parsed_coords if len(coord) == 2]

                    latitudes = [float(coord[1]) for coord in valid_coords]
                    longitudes = [float(coord[0]) for coord in valid_coords]

                    if latitudes and longitudes:
                        avg_lat = float(np.mean(latitudes))
                        avg_lon = float(np.mean(longitudes))
                        results.append([avg_lat, avg_lon])
                    else:
                        results.append(None)
                except (ValueError, SyntaxError):
                    results.append(None)
            else:
                results.append(None)
        else:
            results.append(None)

    return pd.Series(results)


def clean_data(events_df):
    events_df = events_df.dropDuplicates(["id"])

    events_df = events_df.dropna(subset=["id", "geography", "event_type"])

    events_df = events_df.withColumn("updated", to_timestamp(col("updated"))) \
                         .withColumn("created", to_timestamp(col("created")))

    events_df = events_df.withColumn(
        "coordinates",
        process_coordinates_pandas(col("geography.type"),
                                   col("geography.coordinates"))
    )

    events_df = events_df.withColumn("latitude", col("coordinates")[0]) \
                         .withColumn("longitude", col("coordinates")[1]) \
                         .dropna(subset=["latitude", "longitude"]) \
                         .drop("geography", "coordinates")

    return events_df


def main(input_path_events, output):
    events_df = read_data(spark, input_path_events)
    events_df = clean_data(events_df)

    print("Final schema being written to parquet:")
    events_df.printSchema()
    print("Final DF:")
    events_df.show()
    events_df.write.mode("overwrite").parquet(output)


if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName(
        'DriveBC Events Processing').getOrCreate()
    assert spark.version >= '3.0'
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)
