import ast
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, explode, pandas_udf, PandasUDFType
from pyspark.sql.types import ArrayType, DoubleType
import sys
assert sys.version_info >= (3, 5)


def read_data(spark, input_path_events):
    raw_events = spark.read.json(input_path_events)

    events_exploded = raw_events.select(
        explode(col("events")).alias("events_data"))

    events = events_exploded.select("events_data.*")

    return events


@pandas_udf(ArrayType(DoubleType()), PandasUDFType.SCALAR)
def process_coordinates_pandas(geo_type_series, coords_series):
    import pandas as pd
    import numpy as np

    results = []
    for geo_type, coords in zip(geo_type_series, coords_series):
        if geo_type == "Point":
            if isinstance(coords, list) and len(coords) == 2:
                try:
                    results.append([float(coords[0]), float(coords[1])])
                except ValueError:
                    results.append(None)
            else:
                results.append(None)
        elif geo_type == "LineString":
            if isinstance(coords, list) and len(coords) > 0:
                try:
                    parsed_coords = [ast.literal_eval(coord) if isinstance(
                        coord, str) else coord for coord in coords]

                    valid_coords = [coord for coord in parsed_coords if isinstance(
                        coord, list) and len(coord) == 2]

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
