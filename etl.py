from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, trim, when, to_timestamp, explode, regexp_replace
import sys
assert sys.version_info >= (3, 5)


def read_data(spark, input_path_events):
    raw_events = spark.read.json(input_path_events)

    events_exploded = raw_events.select(
        explode(col("events")).alias("events_data"))

    events = events_exploded.select("events_data.*")

    return events


def clean_data(events_df):
    events_df = events_df.withColumns(
        "geography.coordinates",
        when(
            col("geography.type") == "Point",
            col("geography.coordinates").cast(
                "array<double>").cast("array<array<double>>")
        ).when(
            col("geography.type") == "LineString",
            col("geography.coordinates").cast("array<array<double>>")
        ).otherwise(None)
    )

    events_df = events_df.dropDuplicates(["id"])

    events_df = events_df.fillna("unknown", subset=[
                                 "headline", "description", "+ivr_message", "updated", "created", "severity"])

    events_df = events_df.withColumn("updated", to_timestamp(col("updated"))) \
                         .withColumn("created", to_timestamp(col("created")))

    events_df = events_df.withColumn("headline", trim(lower(col("headline")))) \
                         .withColumn("description", trim(lower(col("description")))) \
                         .withColumn("headline", regexp_replace(col("headline"), "[^a-zA-Z0-9 ]", "")) \
                         .withColumn("description", regexp_replace(col("description"), "[^a-zA-Z0-9 ]", ""))

    events_df = events_df.dropna(subset=["id", "event_type"])

    return events_df


def main(input_path_events, output):
    events_df = read_data(spark, input_path_events)
    events_df = clean_data(events_df)

    print("Final schema being written to parquet:")
    events_df.printSchema()
    print("Final DF being written to parquet:")
    events_df.show(truncate=False)
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
