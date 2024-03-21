dataset_path = '../../Datasets/Covid_19'
files_list = ['day_wise.csv', 'country_wise_latest.csv', 'covid_19_clean_complete.csv', 'worldometer_data.csv']
file_to_topic_mapping = {
    'day_wise': 'covid19_day_wise_data',
    'country_wise_latest': 'covid19_country_wise_data',
    'covid_19_clean_complete': 'covid19_total_data',
    'worldometer_data': 'covid19_worldometer_data'
}
kafkaBootstrapServers = '127.0.0.1:9092'

snowflake_params = {
  "sfURL" : "deloittena.us-east-1.snowflakecomputing.com",
  "sfUser" : "sartnagpal",
  "sfPassword" : "Sar@2897",
  "sfDatabase" : "KROGER_HW_POC_DB",
  "sfSchema" : "COVID19_INSIGHTS",
  "sfWarehouse" : "KROGER_HW_POC_WH"
}
SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
monthly_stats_table = 'MONTHLY_2020_STATS'
region_stats_table = 'REGION_STATS'
region_top5_active_stats = 'REGION_ACTIVE_5_STATS'
