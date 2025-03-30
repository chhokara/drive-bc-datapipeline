import numpy as np
import pandas as pd
from sklearn.cluster import DBSCAN
from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType, ArrayType
import sys

KMS_PER_RADIAN = 6371.0088
EPS_KM = 50
EPS_RAD = EPS_KM / KMS_PER_RADIAN
MIN_SAMPLES = 10

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

@pandas_udf(IntegerType())
def coordinate_cluster(lon_series, lat_series):
    coords = np.radians(np.column_stack((lat_series, lon_series)))
    if len(coords) < MIN_SAMPLES:
        return pd.Series([-1] * len(coords))
    
    try:
        db = DBSCAN(eps=EPS_RAD, min_samples=MIN_SAMPLES, metric='haversine').fit(coords)
        return pd.Series(db.labels_)
    except Exception as e:
        print("Clustering error:", e)
        return pd.Series([-1] * len(coords))

    
@pandas_udf(IntegerType())
def severity_cluster(lon_series, lat_series, severity_series):
    coords = np.radians(np.column_stack((lat_series, lon_series)))
    severities = severity_series.map(SEVERITY_MAP).fillna(1).astype(float).values.reshape(-1, 1)

    combined = np.hstack((coords, severities))

    if len(combined) < MIN_SAMPLES:
        return pd.Series([-1] * len(combined))

    try:
        db = DBSCAN(eps=EPS_RAD, min_samples=MIN_SAMPLES, metric='euclidean').fit(combined)
        return pd.Series(db.labels_)
    except Exception as e:
        print("Clustering error:", e)
        return pd.Series([-1] * len(combined))


def main(input_path, output_path_1, output_path_2):
  
    df = spark.read.schema(event_schema).parquet(input_path)
    df = df.dropDuplicates(["id"])
    
    df_with_coords = df.withColumn("cluster", coordinate_cluster(df["longitude"], df["latitude"]))
    
    df = df.filter(df["severity"] != "UNKNOWN")
    df_with_severities = df.withColumn("cluster", severity_cluster(df["longitude"], df["latitude"], df["severity"]))
    
    coord_clusters = df_with_coords.select(
        df_with_coords["longitude"], 
        df_with_coords["latitude"], 
        df_with_coords['cluster']
    )
    
    severity_clusters = df_with_severities.select(
        df_with_severities["longitude"], 
        df_with_severities["latitude"], 
        df_with_severities["severity"], 
        df_with_severities['cluster']
    )
    
    coord_clusters.write.mode("overwrite").parquet(output_path_1)
    
    severity_clusters.write.mode("overwrite").parquet(output_path_2)
    
    
if __name__ == '__main__':
    spark = SparkSession.builder.appName("Clustering Algorithm").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        input_path = sys.argv[1]
        output_path_1 = sys.argv[2]
        output_path_2 = sys.argv[3]
        main(input_path, output_path_1, output_path_2)
    except Exception as e:
        print("ERROR:", e)
        raise

