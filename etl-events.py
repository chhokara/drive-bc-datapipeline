import numpy as np
import pandas as pd
import ast
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, explode, from_json, pandas_udf
from pyspark.sql.types import ArrayType, DoubleType, StringType, StructType, StructField
import sys

event_schema = StructType([
    StructField("id", StringType()),
    StructField("headline", StringType()),
    StructField("status", StringType()),
    StructField("created", StringType()),
    StructField("updated", StringType()),
    StructField("event_type", StringType()),
    StructField("event_subtypes", ArrayType(StringType())),
    StructField("severity", StringType()),
    StructField("geography", StructType([
        StructField("type", StringType()),
        StructField("coordinates", StringType())
    ])),
])

top_level_schema = StructType([
    StructField("events", ArrayType(event_schema))
])

@pandas_udf(ArrayType(DoubleType()))
def process_coordinates_pandas(geo_type_series, coords_series):
    results = []
    for geo_type, coords_str in zip(geo_type_series, coords_series):
        try:
            parsed = ast.literal_eval(coords_str)
        except (ValueError, SyntaxError):
            results.append(None)
            continue

        if geo_type == "Point":
            if isinstance(parsed, list) and len(parsed) == 2:
                try:
                    results.append([float(parsed[0]), float(parsed[1])])
                except ValueError:
                    results.append(None)
            else:
                results.append(None)

        elif geo_type == "LineString":
            if isinstance(parsed, list) and all(isinstance(coord, list) and len(coord) == 2 for coord in parsed):
                try:
                    latitudes = [float(coord[1]) for coord in parsed]
                    longitudes = [float(coord[0]) for coord in parsed]
                    avg_lat = float(np.mean(latitudes))
                    avg_lon = float(np.mean(longitudes))
                    results.append([avg_lon, avg_lat])
                except ValueError:
                    results.append(None)
            else:
                results.append(None)

        else:
            results.append(None)

    return pd.Series(results)

# [lon, lat] pairs for plotting 
    
def main(kinesis_stream_name, output_path):
  
    print('stream name', kinesis_stream_name)
    print('output path', output_path)
    
    # Step 1: Read from Kinesis stream
    raw_df = spark.readStream \
        .format("aws-kinesis") \
        .option("kinesis.streamName", kinesis_stream_name) \
        .option("kinesis.region", "us-west-2") \
        .option("kinesis.endpointUrl", "https://kinesis.us-west-2.amazonaws.com") \
        .option("kinesis.initialPosition", "LATEST") \
        .load()

    # Step 2: Extract JSON from binary
    json_df = raw_df.selectExpr("CAST(data AS STRING) as json_str")

    # Step 3: Parse JSON
    parsed_df = json_df.select(from_json(col("json_str"), top_level_schema).alias("parsed"))
    
    # Step 4: Explode events and attirbutes into a tabular format
    exploded_df = parsed_df \
        .selectExpr("parsed.events as events") \
        .select(explode("events").alias("event")) \
        .select("event.*")    
        
    # Step 5: Parse coordinates using UDF
    exploded_df = exploded_df.withColumn(
    "coordinates", process_coordinates_pandas(
        col("geography.type"),
        col("geography.coordinates")
    )
    ).withColumn("latitude", col("coordinates")[1]) \
    .withColumn("longitude", col("coordinates")[0])
    
    # Step 6: Drop NAs & duplicates, convert to timestamps, and drop redundant columns
    cleaned_df = exploded_df.dropDuplicates(["id"]) \
        .dropna(subset=["id", "geography", "event_type"]) \
        .withColumn("updated", to_timestamp(col("updated"))) \
        .withColumn("created", to_timestamp(col("created"))) \
        .dropna(subset=["latitude", "longitude"]) \
        .drop("geography", "coordinates")
        
        
    cleaned_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .start() \
        .awaitTermination()
    

    # # Write to Parquet sink in micro-batches
    # print('writing to parquet!')
    # query = cleaned_df.writeStream \
    #     .format("parquet") \
    #     .option("checkpointLocation", output_path + "/_checkpoint") \
    #     .option("path", output_path) \
    #     .outputMode("append") \
    #     .start()

    # query = values.writeStream \
    # .outputMode('append') \
    # .format('console') \
    # .option('truncate', False) \
    # .start()
    
    # query.awaitTermination()


if __name__ == '__main__':
    # Create Spark session
    spark = SparkSession.builder.appName("DriveBC Kinesis Streaming").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        print("Args:", sys.argv)
        kinesis_stream_name = sys.argv[1]
        output_path = sys.argv[2]
        main(kinesis_stream_name, output_path)
    except Exception as e:
        print("ERROR:", e)
        raise

