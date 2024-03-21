from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json,col
from config.constants import kafkaBootstrapServers,file_to_topic_mapping
from config.schema import day_wise_schema
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 pyspark-shell'
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 pyspark-shell'
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.kafka:kafka-clients:2.8.1'

spark = SparkSession.builder.appName('kafka-consumer-covid19')\
    .getOrCreate()
    #.config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")\

spark.sparkContext.setLogLevel("ERROR")

day_wise_df = spark.readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers",kafkaBootstrapServers)\
    .option("subscribe","covid19_day_wise_data")\
    .option("startingOffsets", "earliest")\
    .load()

#day_wise_df.printSchema()
# day_wise_df\
#     .writeStream\
#     .format("parquet")\
#     .option("checkpointLocation", "config/chkpoint_dir")\
#     .start("config/output")\
#     .awaitTermination()

parsed_day_wise_df = day_wise_df.selectExpr('CAST(value as string)')\
    .select(from_json('value',day_wise_schema).alias('day_wise'))\
    .select('day_wise.*')

#.trigger(processingTime='5 seconds')\
parsed_stream = parsed_day_wise_df.\
    writeStream\
    .trigger(processingTime='5 seconds')\
    .option("checkpointLocation","checkpt")\
    .format("parquet")\
    .outputMode("append")\
    .start("day_wise_file")
#.option("checkpointLocation","checkpt")\
    #.format("console").start()

parsed_stream.awaitTermination()
#     .writeStream \
#     .format("console").start()
   # .outputMode('append')\

print('Streaming completed')

