import logging
import threading
from pyspark.sql import DataFrame

from confluent_kafka import Producer
from config.constants import dataset_path, file_to_topic_mapping,kafkaBootstrapServers
import csv
import json

produced_msg_counts = {}
original_msg_counts = {}

producer = Producer({'bootstrap.servers': kafkaBootstrapServers, 'queue.buffering.max.messages': 1000000,
                     'queue.buffering.max.ms': 500,
                     'batch.num.messages': 50,
                     'default.topic.config': {'acks': 'all'}})



def csv_to_json(file_name):
    with open(f'{dataset_path}/{file_name}', 'r', encoding='utf-8') as file:
        csv_file = csv.DictReader(file)
        for row in csv_file:
            yield json.dumps(row)


def kafka_ack(err, msg):
    if err is not None:
        logging.error(f'Failed to produce msg : {str(msg)} : {str(err)}')
    else:
        logging.debug(f'Message produced : {str(msg)}')


def produce_to_kafka(file_name):
    for json_row in csv_to_json(file_name):
        producer.produce(topic=file_to_topic_mapping[file_name.split('.')[0]],
                         value=json_row.encode('utf-8'), callback=kafka_ack)

    producer.flush()


def fetch_file_rows_count(files_list):
    for file in files_list:
        with open(f'{dataset_path}/{file}', 'r') as f:
            csv_file = csv.DictReader(f)
            count = 0
            for row in csv_file:
                count = count + 1
        original_msg_counts[file] = count


def fetch_data(spark, filepath, schema=None) -> DataFrame:
    if schema is not None:
        return spark.read.format('csv').schema(schema).option('header', 'true').load(filepath)
    else:
        return spark.read.format('csv').options(**{'header':'true','inferschema':'true'}).load(filepath)
