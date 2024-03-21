from pyspark.sql.types import StructType,StructField,DateType,IntegerType,DoubleType,StringType

day_wise_schema = StructType([
    StructField('Date',DateType(),True),
    StructField('Confirmed',IntegerType(),True),
    StructField('Deaths',IntegerType(),True),
    StructField('Recovered',IntegerType(),True),
    StructField('Active',IntegerType(),True),
    StructField('New cases',IntegerType(),True),
    StructField('New deaths',IntegerType(),True),
    StructField('New recovered',IntegerType(),True),
    StructField('Deaths / 100 Cases',DoubleType(),True),
    StructField('Recovered / 100 Cases',DoubleType(),True),
    StructField('Deaths / 100 Recovered',DoubleType(),True),
    StructField('No. of countries',IntegerType(),True)
     ])


divvy_cycles_schema = StructType([
    StructField('ID',StringType(),True),
    StructField('Station Name',StringType(),True),
    StructField('Short Name',StringType(),True),
    StructField('Total Docks',IntegerType(),True),
    StructField('Docks in Service',IntegerType(),True),
    StructField('Status',StringType(),True),
    StructField('Latitude',DoubleType(),True),
    StructField('Longitude',DoubleType(),True)
])