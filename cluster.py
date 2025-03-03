import sys
from pyspark.sql import SparkSession
import pandas as pd
import ast
from sklearn.cluster import DBSCAN
import numpy as np

def cluster(input):
    df = spark.read.parquet(input)
    df.show()
    # df_spark = df_spark.na.drop(subset=['geography.coordinates'])
    # df_spark = df_spark.select("geography.coordinates", "event_type")

    # def parse_coordinates(coord):
    #     if isinstance(coord, str):
    #         try:
    #             parsed = ast.literal_eval(coord)
    #             if isinstance(parsed, list) and len(parsed) > 0 and isinstance(parsed[0], list):
    #                 return parsed[0]
    #             return parsed
    #         except:
    #             return None
    #     return coord

    # parse_coordinates_udf = udf(parse_coordinates, ArrayType(DoubleType()))


if __name__ == '__main__':
    input = sys.argv[1]
    spark = SparkSession.builder.appName(
        'DriveBC Events Clustering').getOrCreate()
    assert spark.version >= '3.0'
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    cluster(input)