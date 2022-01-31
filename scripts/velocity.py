from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql.functions import *
import os
import pandas as pd
import numpy as np
from scipy.spatial import distance

spark = SparkSession.builder.appName("Velocity").getOrCreate()
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 pyspark-shell'

df2 = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "instance-tram-1:9092") \
  .option("subscribe", "tram_positions_processed") \
  .option('failOnDataLoss', 'false') \
  .load()

schema = StructType([
    StructField('Lines', StringType()),
    StructField('Lon', FloatType()),
    StructField('VehicleNumber', StringType()),
    StructField('Time', TimestampType()),
    StructField('Lat', FloatType()),
    StructField('Brigade', StringType())
])

table2 = df2.selectExpr("CAST(key AS STRING)","CAST(value AS STRING)").select(from_json('value', schema).alias('temp')).select('temp.*')
table2 = table2.withColumnRenamed('Lines', 'line')\
.withColumnRenamed('Lon', 'lon2')\
.withColumnRenamed('Time', 'time2')\
.withColumnRenamed('Lat', 'lat2')

table2 = table2.withColumn('coords2', array(table2.lon2, table2.lat2))
table2 = table2.withColumn('line', table2.line.cast(IntegerType()))

table3 = table2 \
    .withWatermark("time2", "5 minutes") \
    .groupBy(
        window(table2.time2, "1 minute", "30 seconds"),
        table2.line,
        table2.Brigade) \
    .agg((max(table2.lon2) - min(table2.lon2)).alias('A'), (max(table2.lat2) - min(table2.lat2)).alias('B'))\
    .withColumn('length', sqrt(col('A')**2 + col('B')**2))\
    .withColumn('velocity', col('length') * 111.139 * 60)\
    .select('window', 'line', 'Brigade', 'velocity')

table3.selectExpr("to_json(struct(*)) AS value")\
.writeStream\
.option('checkpointLocation', 'tmp')\
.format("kafka")\
.option("kafka.bootstrap.servers", "instance-tram-1:9092")\
.option("topic", "velocity")\
.outputMode('complete')\
.start()
