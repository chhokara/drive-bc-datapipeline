"""
This file performs various simple analyses on the cleaned archived event data.
"""

from pyspark.sql import SparkSession, functions as f
import sys
assert sys.version_info >= (3, 5)

def by_group(df, group, output):
    """
    For a given dataframe and column name, writes a parquet file with count grouping.
    """
    grp_df = df.groupBy(group).count()
    grp_df.write.mode('overwrite').parquet(f'{output}/by_{group}')


def main(inputs,  output):
    df = spark.read.parquet(inputs)
    by_group(df, 'severity', output)
    by_group(df, 'event_type', output)
    by_group(df, 'road', output)
    by_group(df, 'area', output)


if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('Simple Analytics').getOrCreate()
    assert spark.version >= '3.0'
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)