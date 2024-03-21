from pyspark.sql import SparkSession
from config.schema import divvy_cycles_schema
spark = SparkSession.builder.appName('spark_streaming_file').getOrCreate()

df = spark.readStream\
    .format('csv')\
    .schema(divvy_cycles_schema)\
    .option('header','true')\
    .load('C:\\Datasets\\Divvy_Bicycle_Stations_20240206.csv')

df.writeStream\
    .format('parquet')\
    .outputMode('append')\
    .option('checkpointLocation','checkpt')\
    .start('divvy_file')