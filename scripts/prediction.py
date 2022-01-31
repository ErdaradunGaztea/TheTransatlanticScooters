from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.conf import SparkConf
from pyspark.ml import PipelineModel

spark = SparkSession.builder.appName("Prediction") \
        .config("spark.dynamicAllocation.enabled", "false") \
        .config("spark.executor.memory","1000m") \
        .getOrCreate()

schema = StructType([
    StructField('dt', TimestampType()),
    StructField('temp', FloatType()),
    StructField('pressure', IntegerType()),
    StructField('humidity', IntegerType()),
    StructField('visibility', IntegerType()),
    StructField('wind_speed', FloatType()),
    StructField('clouds', IntegerType()),
    StructField('rain.1h', FloatType(), False),
    StructField('snow.1h', FloatType(), False),
    StructField('pop', FloatType())
])

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "instance-tram-1:9092") \
    .option("subscribe", "weather_data_processed") \
    .option("failOnDataLoss", "false") \
    .load()
table = df.selectExpr("CAST(key AS STRING)","CAST(value AS STRING)").select(from_json('value', schema).alias('tmp')).select('tmp.*')

table = table.withColumn('dayofweek', dayofweek(col('dt')))\
            .withColumn('minuteofday', minute(col('dt')) + 60 * hour(col('dt')))\
.withColumnRenamed('wind_speed', 'wind')\
.withColumnRenamed('clouds', 'cloud')\
.withColumnRenamed('rain.1h', 'rain')\
.withColumnRenamed('snow.1h', 'snow')\
.withColumn('line', lit(15))\
.fillna(value=0)

model = PipelineModel.load('regression_model')

predictions = model.transform(table)

predictions.selectExpr("to_json(struct(*)) AS value")\
.writeStream\
.option('checkpointLocation', 'tmp')\
.format("kafka")\
.outputMode("complete")\
.option("kafka.bootstrap.servers", "instance-tram-1:9092")\
.option("topic", "velocity")\
.start()