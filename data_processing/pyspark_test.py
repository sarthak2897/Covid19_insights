from pyspark.sql import SparkSession, DataFrame,Row
import pyspark.sql.functions as f

spark = SparkSession.builder.appName('pyspark_test').getOrCreate()

df: DataFrame = spark.createDataFrame([Row(id = 1,value = {"a": "Test", "b": "XYZ", "c": [1,2,3,4]})])
#df.show(truncate=False)
df.select('id','value.a','value.b',f.explode(df.value.c)).collect()
#,f.explode(f.col('value.c'))