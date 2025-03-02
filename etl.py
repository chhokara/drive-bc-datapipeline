from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, DoubleType
from pyspark.sql.functions import col, lower, trim, regexp_replace, to_timestamp, explode, udf
import sys
assert sys.version_info >= (3, 5)


def normalize_coordinates(geo_type, coords):
    try:
        if geo_type == "Point":
            return [[float(coords[0]), float(coords[1])]]
        elif geo_type == "LineString":
            return [[float(p[0]), float(p[1])] for p in coords]
        else:
            return None
    except:
        return None


normalize_coordinates_udf = udf(
    normalize_coordinates, ArrayType(ArrayType(DoubleType())))


def read_data(spark, input_path_events):
    raw_events = spark.read.json(input_path_events)

    events_exploded = raw_events.select(
        explode(col("events")).alias("events_data"))

    events = events_exploded.select("events_data.*")

    events = events.withColumn("geography.coordinates",
                               normalize_coordinates_udf(col("geography.type"), col("geography.coordinates")))

    return events


def clean_data(events_df):
    events_df = events_df.dropDuplicates(["id"])

    events_df = events_df.fillna("unknown", subset=[
                                 "headline", "description", "ivr_message", "updated", "created", "severity"])

    events_df = events_df.withColumn("updated", to_timestamp(col("updated"))) \
                         .withColumn("created", to_timestamp(col("created")))

    events_df = events_df.withColumn("headline", trim(lower(col("headline")))) \
                         .withColumn("description", trim(lower(col("description")))) \
                         .withColumn("headline", regexp_replace(col("headline"), "[^a-zA-Z0-9 ]", "")) \
                         .withColumn("description", regexp_replace(col("description"), "[^a-zA-Z0-9 ]", ""))

    events_df = events_df.filter((col("geography.coordinates")[0][0] >= -180) & (col("geography.coordinates")[0][0] <= 180) &
                                 (col("geography.coordinates")[0][1] >= -90) & (col("geography.coordinates")[0][1] <= 90))

    events_df = events_df.dropna(subset=["id", "event_type"])

    events_df = events_df.filter(col("geography").isNotNull())

    return events_df


def define_schema():
    events_schema = StructType([
        StructField("url", StringType(), True),
        StructField("id", StringType(), True),
        StructField("jurisdiction_url", StringType(), True),
        StructField("headline", StringType(), True),
        StructField("status", StringType(), True),
        StructField("description", StringType(), True),
        StructField("ivr_message", StringType(), True),
        StructField("linear_reference_km", DoubleType(),
                    True),
        StructField("schedule", StructType([
            StructField("intervals", ArrayType(StringType()), True),
            StructField("recurring_schedules", ArrayType(StructType([
                StructField("daily_end_time", StringType(), True),
                StructField("daily_start_time", StringType(), True),
                StructField("days", ArrayType(DoubleType()), True),
                StructField("end_date", StringType(), True),
                StructField("start_date", StringType(), True)
            ])), True)
        ]), True),
        StructField("event_type", StringType(), True),
        StructField("event_subtypes", ArrayType(StringType()), True),
        StructField("updated", StringType(), True),
        StructField("created", StringType(), True),
        StructField("severity", StringType(), True),
        StructField("geography", StructType([
            StructField("type", StringType(), True),
            StructField("coordinates", ArrayType(
                ArrayType(DoubleType())), True)
        ]), True),
        StructField("roads", ArrayType(StructType([
            StructField("direction", StringType(), True),
            StructField("from", StringType(), True),
            StructField("name", StringType(), True),
            StructField("to", StringType(), True),
            StructField("state", StringType(), True),
            StructField("delay", StringType(), True)
        ])), True),
        StructField("areas", ArrayType(StructType([
            StructField("url", StringType(), True),
            StructField("name", StringType(), True),
            StructField("id", StringType(), True)
        ])), True)
    ])

    return events_schema


def main(input_path_events, output):
    events_df = read_data(spark, input_path_events)
    events_schema = define_schema()
    events_df = spark.createDataFrame(events_df.rdd, schema=events_schema)
    events_df = clean_data(events_df)

    print("Final DF being written to parquet:")
    events_df.printSchema()
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
