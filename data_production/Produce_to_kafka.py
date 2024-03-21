from config.utils import *
from config.constants import files_list
import logging


def produce_all_to_kafka(files_list):
    for file in files_list:
        produce_to_kafka(file)


if __name__ == '__main__':
    try:
        logging.getLogger().setLevel(logging.INFO)
        fetch_file_rows_count(files_list)
        produce_all_to_kafka(files_list)
        logging.info('Covid 19 data successfully produced to kafka, ready for consumption')
        logging.info(f'Original counts : {original_msg_counts}')

    except Exception as e:
        logging.exception(f'Error occurred while producing data from csv to json')
