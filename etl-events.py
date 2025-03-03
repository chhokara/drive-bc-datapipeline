import ast
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, explode, udf
from pyspark.sql.types import ArrayType, DoubleType
import sys
assert sys.version_info >= (3, 5)


def read_data(spark, input_path_events):
    raw_events = spark.read.json(input_path_events)

    events_exploded = raw_events.select(
        explode(col("events")).alias("events_data"))

    events = events_exploded.select("events_data.*")

    return events


def process_coordinates(geo_type, coords):
    if geo_type == "Point":
        if isinstance(coords, list) and len(coords) == 2:
            try:
                return [float(coords[0]), float(coords[1])]
            except ValueError:
                return None
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
                    return [avg_lat, avg_lon]
            except (ValueError, SyntaxError):
                return None
    return None


process_coordinates_udf = udf(process_coordinates, ArrayType(DoubleType()))


def clean_data(events_df):
    events_df = events_df.dropDuplicates(["id"])

    events_df = events_df.fillna("unknown", subset=[
                                 "headline", "description", "+ivr_message", "severity"])

    events_df = events_df.dropna(subset=["id", "geography", "event_type"])

    events_df = events_df.withColumn("updated", to_timestamp(col("updated"))) \
                         .withColumn("created", to_timestamp(col("created")))

    events_df = events_df.filter(col("geography.type").isNotNull() & col(
        "geography.coordinates").isNotNull())

    events_df = events_df.withColumn(
        "coordinates",
        process_coordinates_udf(col("geography.type"),
                                col("geography.coordinates"))
    )

    events_df = events_df.withColumn("latitude", col("coordinates")[0]) \
                         .withColumn("longitude", col("coordinates")[1])

    events_df = events_df.dropna(subset=["latitude", "longitude"])

    events_df = events_df.drop("geography", "coordinates")

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
