import mysql.connector
import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import sys
from pyspark.mllib.linalg import Vectors
from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.streaming import StreamingContext
import pyspark.sql.types as tp
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.sql import Row
from pyspark.ml.evaluation import RegressionEvaluator

spark = SparkSession.builder.appName("Training") \
        .config("spark.dynamicAllocation.enabled", "false") \
        .config("spark.executor.memory","5000m") \
        .getOrCreate()

# Establish a connection
conn_weather = mysql.connector.connect(user='root', password='BigDataAnalytics',
        host='localhost:3309',
        database='weather_aggregates',
        unix_socket='/home/dataproc/sockets/spring-cab-332810:europe-west1:serving-mysql')

cursor_weather = conn_weather.cursor()
query = "SELECT * FROM weather_aggregates"
# Create a pandas dataframe
pdf_weather = pd.read_sql(query, con=conn_weather)

# Convert Pandas dataframe to spark DataFrame
schema_weather = StructType([
    StructField('dt', IntegerType()),
    StructField('temp', FloatType()),
    StructField('pressure', IntegerType()),
    StructField('humidity', IntegerType()),
    StructField('visibility', IntegerType()),
    StructField('wind', FloatType()),
    StructField('cloud', IntegerType()),
    StructField('rain', FloatType(), False),
    StructField('snow', FloatType(), False),
    StructField('pop', FloatType())
])
df1 = spark.createDataFrame(pdf_weather, schema=schema_weather)
df1 = df1.withColumn('dt', df1.dt.cast(TimestampType()))
df1 = df1.withColumn('dt', df1.dt.cast(TimestampType()))\
        .withColumn('second', second(col('dt')))\
        .withColumn('offset', col('second'))\
        .withColumn('dt2', (col('dt')).cast(IntegerType()) - col('offset'))\
        .withColumn('dt2', from_unixtime(col('dt2')))\
        .select('dt', 'temp', 'pressure', 'humidity', 'visibility', 'wind', 'cloud', 'rain', 'snow', 'pop')

cursor_weather.close()
conn_weather.close()

conn_velocity = mysql.connector.connect(user='root', password='BigDataAnalytics',
        host='localhost:3309',
        database='velocity',
        unix_socket='/home/dataproc/sockets/spring-cab-332810:europe-west1:serving-mysql')

cursor_velocity = conn_velocity.cursor()
query = "SELECT * FROM Velocity"
# Create a pandas dataframe
pdf = pd.read_sql(query, con=conn_velocity)

# Convert Pandas dataframe to spark DataFrame
schema_velocity = StructType([
    StructField('dt', IntegerType()),
    StructField('line', StringType()),
    StructField('brigade', StringType()),
    StructField('velocity', FloatType())
])
df2 = spark.createDataFrame(pdf, schema=schema_velocity)
df2 = df2.withColumn('dt', df2.dt.cast(TimestampType()))\
        .withColumn('second', second(col('dt')))\
        .withColumn('offset', col('second'))\
        .withColumn('dt2', (col('dt')).cast(IntegerType()) - col('offset'))\
        .withColumn('dt2', from_unixtime(col('dt2')))\
        .withColumn('dayofweek', dayofweek(col('dt2')))\
        .withColumn('minuteofday', minute(col('dt2')) + 60 * hour(col('dt2')))\
        .withColumn('line', df2.line.cast(IntegerType()))\
        .withColumn('brigade', df2.brigade.cast(IntegerType()))\
        .select('dt', 'dt2', 'line', 'brigade', 'velocity', 'dayofweek', 'minuteofday')
        

cursor_velocity.close()
conn_velocity.close()

df3 = df1.join(df2, on='dt')

from pyspark.ml.regression import DecisionTreeRegressor
from pyspark.ml import Pipeline


vectorAssembler = VectorAssembler(inputCols = ['dayofweek', 'minuteofday', 'temp', 'pressure', 'humidity', 'visibility', 'wind', 'cloud', 'rain', 'snow', 'pop', 'line'], outputCol = 'features')
dt = DecisionTreeRegressor(featuresCol = 'features', labelCol='velocity')

pipeline = Pipeline(stages=[vectorAssembler, dt])

modelFit = pipeline.fit(df3)

modelFit.write().overwrite().save('regression_model')

spark.stop()