from pyspark.sql import SparkSession, functions as f
import sys
assert sys.version_info >= (3, 5)

def clean_df(df):
    df = df.dropDuplicates(['id'])
    df = df.withColumn('road', f.explode('roads'))
    df = df.withColumn('area', f.explode('areas'))
    col_filt_df = df.select([
        'event_type', 
        'created', 
        'severity', 
        f.col('road.name').alias('road'),
        f.col('area.name').alias('area')
    ])
    return col_filt_df


def main(inputs,  output):
    raw_df = spark.read \
        .option('multiLine', 'true') \
        .json(inputs)
    cleaned_df = clean_df(raw_df)
    cleaned_df.write.mode('overwrite').parquet(output)


if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('ETL').getOrCreate()
    assert spark.version >= '3.0'
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)