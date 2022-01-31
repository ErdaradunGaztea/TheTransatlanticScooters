from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.conf import SparkConf
spark = SparkSession.builder.appName("Weather") \
        .config("spark.dynamicAllocation.enabled", "false") \
        .config("spark.executor.memory","1000m") \
        .getOrCreate()

schema = StructType([
    StructField('dt', IntegerType()),
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
    .option("subscribe", "weather_data_curr") \
    .option("failOnDataLoss", "false") \
    .load()
table = df.selectExpr("CAST(key AS STRING)","CAST(value AS STRING)").select(from_json('value', schema).alias('tmp')).select('tmp.*')
df_kafka = table\
    .withColumn('value', to_json(struct(table["`dt`"], table["`temp`"], table["`pressure`"], table["`humidity`"], table["`visibility`"],
                                       table["`wind_speed`"], table["`clouds`"], table["`rain.1h`"], table["`snow.1h`"], table["`pop`"])))
query = df_kafka.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "instance-tram-1:9092") \
    .option("topic", "weather-aggregates") \
    .option("checkpointLocation", "~/agg_checkpoint/") \
    .start() \
    .awaitTermination()

spark.stop()
