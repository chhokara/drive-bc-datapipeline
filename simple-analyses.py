"""
This file performs various simple analyses on the cleaned archived event data.
"""

from pyspark.sql import SparkSession, functions as f
import sys
assert sys.version_info >= (3, 5)

def by_severity(df, output):
    grp_df = df.groupBy('severity').count()
    grp_df.write.mode('overwrite').parquet(f'{output}/by_severity')

def main(inputs,  output):
    df = spark.read.parquet(inputs)
    by_severity(df, output)


if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('Simple Analytics').getOrCreate()
    assert spark.version >= '3.0'
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)