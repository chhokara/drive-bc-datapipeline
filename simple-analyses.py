"""
This file performs various simple analyses on the cleaned archived event data.
"""

from pyspark.sql import SparkSession, functions as f
import sys
assert sys.version_info >= (3, 5)

def by_group(df, groups, output):
    """
    For a given dataframe and column name(s) (i.e. string or list of strings), writes a parquet file with count grouping.
    """
    grp_df = df.groupBy(groups).count()
    grp_df = grp_df.orderBy(groups)
    label = groups
    if (isinstance(groups, list)):
        label = '_'.join(groups)
        print(label)
    grp_df.write.mode('overwrite').parquet(f'{output}/by_{label}')


def main(inputs,  output):
    df = spark.read.parquet(inputs)
    df = df.withColumn('year', f.year('timestamp'))
    df = df.withColumn('month', f.month('timestamp'))
    df = df.withColumn('hour', f.hour('timestamp'))
    by_group(df, 'severity', output)
    by_group(df, 'event_type', output)
    by_group(df, 'road', output)
    by_group(df, 'area', output)
    by_group(df, ['event_type', 'month', 'year'], output)


if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('Simple Analytics').getOrCreate()
    assert spark.version >= '3.0'
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)