from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, sum, round, rank
from pyspark.sql.types import IntegerType
from config.constants import SNOWFLAKE_SOURCE_NAME, region_stats_table, snowflake_params, region_top5_active_stats
from config.utils import fetch_data
import os

os.environ[
    'PYSPARK_SUBMIT_ARGS'] = '--packages net.snowflake:snowflake-jdbc:3.13.22,net.snowflake:spark-snowflake_2.12:2.11.0-spark_3.3 pyspark-shell'

spark = SparkSession.builder.appName('Covid19_country_wise').getOrCreate()

spark.sparkContext.setLogLevel('ERROR')

country_wise_df = fetch_data(spark, 'C:\\Datasets\\Covid_19\\country_wise_latest.csv')

# New cases, deaths,recovered in percent
total_new_cases = country_wise_df.select(sum('New cases')).collect()[0][0]
total_deaths = country_wise_df.select(sum('Deaths')).collect()[0][0]
total_recovered = country_wise_df.select(sum('Recovered')).collect()[0][0]

region_wise_df = country_wise_df.groupBy('WHO Region') \
    .agg(round((sum(col('New cases').cast(IntegerType())) / total_new_cases) * 100, 2).alias('new_cases_percent'),
         round((sum(col('Deaths').cast(IntegerType())) / total_deaths) * 100, 2).alias('deaths_percent'),
         round((sum(col('Recovered').cast(IntegerType())) / total_recovered) * 100, 2).alias('recovered_percent')) \
    .orderBy(col('deaths_percent').desc(), col('new_cases_percent').desc(), col('recovered_percent').desc()) \
    .withColumnRenamed('WHO Region', 'Region')

region_wise_df.write \
    .format(SNOWFLAKE_SOURCE_NAME) \
    .options(**snowflake_params) \
    .option('dbTable', region_stats_table) \
    .mode('overwrite') \
    .save()

print(f'Region stats written to snowflake table {region_stats_table}')

renamed_columns = {
    'Country/Region': 'Country',
    'WHO Region': 'Region'
}

# Fetch top country per region where there are most active cases
window = Window.partitionBy(col('WHO Region')).orderBy((col('Active').cast(IntegerType())).desc())
top_country_region_df = country_wise_df.withColumn('rank', rank().over(window)) \
    .filter(col('rank') < 6) \
    .select('Country/Region', 'Confirmed', 'Deaths', 'Recovered', 'Active', 'WHO Region', 'rank') \
    .orderBy(col('rank'))

top_country_region_df = top_country_region_df \
    .select([col(c).alias(renamed_columns.get(c))
             if c in renamed_columns.keys()
             else col(c) for c in top_country_region_df.columns])

top_country_region_df.write \
    .format(SNOWFLAKE_SOURCE_NAME) \
    .options(**snowflake_params) \
    .option('dbTable', region_top5_active_stats) \
    .mode('overwrite') \
    .save()

print(f'Region top 5 active cases stats written to snowflake table {region_top5_active_stats}')

# top_country_region_df.show(top_country_region_df.count(),truncate=False)
