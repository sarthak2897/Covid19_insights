from pyspark.sql import SparkSession
from pyspark.sql.functions import month, col, sum, round, udf
from config.schema import day_wise_schema
from config.constants import snowflake_params,monthly_stats_table,SNOWFLAKE_SOURCE_NAME
from config.utils import fetch_data
import os

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages net.snowflake:snowflake-jdbc:3.13.22,net.snowflake:spark-snowflake_2.12:2.11.0-spark_3.3 pyspark-shell'

spark = SparkSession.builder.appName('covid19_insights').getOrCreate()

spark.sparkContext.setLogLevel('ERROR')


day_wise_df = fetch_data(spark,'C:\\Datasets\\Covid_19\\day_wise.csv', day_wise_schema)

clean_day_wise_df = day_wise_df.fillna(0, ['Confirmed', 'Deaths', 'Recovered', 'Active'])
total_deaths = clean_day_wise_df.select(sum('Deaths')).collect()[0][0]
total_recovered = clean_day_wise_df.select(sum('Recovered')).collect()[0][0]
total_active = clean_day_wise_df.select(sum('Active')).collect()[0][0]


def get_month(month_no):
    if month_no == 1:
        return 'January'
    elif month_no == 2:
        return 'February'
    elif month_no == 3:
        return 'March'
    elif month_no == 4:
        return 'April'
    elif month_no == 5:
        return 'May'
    elif month_no == 6:
        return 'June'
    elif month_no == 7:
        return 'July'
    elif month_no == 8:
        return 'August'
    elif month_no == 9:
        return 'September'
    elif month_no == 10:
        return 'October'
    elif month_no == 11:
        return 'November'
    elif month_no == 12:
        return 'December'


month_convert_udf = udf(lambda month_no: get_month(month_no))

year_wise_df = day_wise_df.groupby(month_convert_udf(month(col('Date'))).alias('Month')) \
    .agg(round(((sum(col('Deaths')) / total_deaths) * 100), 2).alias('death_percent_per_month'),
         round(((sum(col('Recovered')) / total_deaths) * 100), 2).alias('recovery_percent_per_month'),
         round(((sum(col('Active')) / total_active) * 100), 2).alias('active_percent_per_month')) \


year_wise_df.write\
    .format(SNOWFLAKE_SOURCE_NAME)\
    .options(**snowflake_params)\
    .option('dbTable',monthly_stats_table)\
    .mode('overwrite')\
    .save()

print('Monthly stats written to snowflake table MONTHLY_2020_STATS')

spark.stop()
