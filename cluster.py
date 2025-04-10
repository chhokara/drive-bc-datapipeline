import numpy as np
import pandas as pd
from sklearn.cluster import DBSCAN
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, ArrayType
import sys

KMS_PER_RADIAN = 6371.0088
EPS_KM = 25
EPS_RAD = EPS_KM / KMS_PER_RADIAN
MIN_SAMPLES = 5

SEVERITY_MAP = {
    "MINOR": 1,
    "MODERATE": 2,
    "MAJOR": 3
}

event_schema = StructType([
    StructField("id", StringType(), True),
    StructField("headline", StringType(), True),
    StructField("status", StringType(), True),
    StructField("created", TimestampType(), True),
    StructField("updated", TimestampType(), True),
    StructField("event_type", StringType(), True),
    StructField("event_subtypes", ArrayType(StringType()), True),
    StructField("severity", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
])
    
# Need to use pandas and avoid parallel processing, or else DBSCAN fails
def main(input, output_1, output_2):
    df = spark.read.schema(event_schema).parquet(input)
    df = df.dropDuplicates(["id"])
    
    df_coords = df.select("id", "latitude", "longitude").dropna().toPandas()
    coords = np.radians(df_coords[['latitude', 'longitude']].values)

    if len(coords) < MIN_SAMPLES:
        df_coords['cluster'] = -1
    else:
        db = DBSCAN(eps=EPS_RAD, min_samples=MIN_SAMPLES, metric='haversine').fit(coords)
        df_coords['cluster'] = db.labels_


    df = df.filter(df["severity"] != "UNKNOWN")
    df_severities = df.select("id", "latitude", "longitude", "severity").dropna().toPandas()    
    df_severities["severity_numeric"] = df_severities["severity"].map(SEVERITY_MAP).fillna(1).astype(float)
    coords = np.radians(df_severities[['latitude', 'longitude']].values)    
    severities = df_severities["severity_numeric"].values.reshape(-1, 1)
    combined = np.hstack((coords, severities))
    
    if len(combined) < MIN_SAMPLES:
        df_severities["cluster"] = -1
    else:
        db = DBSCAN(eps=EPS_RAD, min_samples=MIN_SAMPLES, metric='euclidean').fit(combined)
        df_severities["cluster"] = db.labels_
    
    
    df_coords_spark = spark.createDataFrame(df_coords)
    df_coords_spark.write.mode("overwrite").parquet(output_1)

    df_severities_spark = spark.createDataFrame(df_severities)
    df_severities_spark.write.mode("overwrite").parquet(output_2)
    spark.stop()
    
if __name__ == '__main__':
    spark = SparkSession.builder.appName("Clustering Algorithm").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        input = sys.argv[1]
        output_1 = sys.argv[2]
        output_2 = sys.argv[3]
        main(input, output_1, output_2)
    except Exception as e:
        print("ERROR:", e)
        raise

