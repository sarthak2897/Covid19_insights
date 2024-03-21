from pyspark.sql import SparkSession,Window
from config.utils import fetch_data
from pyspark.sql.functions import col,dense_rank,sum

spark = SparkSession.builder.appName('covid19_complete_dataset').getOrCreate()

spark.sparkContext.setLogLevel('ERROR')

covid_df = fetch_data(spark, 'C:\\Datasets\\Covid_19\\covid_19_clean_complete.csv')

# Stats per country - percent column increase/decrease per 3 months
window = Window.partitionBy(col('Country/Region'),col('Date')).orderBy(col('Date'))
stats_covid_df = covid_df.withColumn('Confirmed',sum(col('Confirmed')).over(window))
stats_covid_df.show(truncate=False)