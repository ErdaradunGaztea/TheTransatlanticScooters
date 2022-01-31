
from pyspark import *
from pyspark.sql import *
from pyspark.sql.utils import *
from pyspark.streaming import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

conf = SparkConf().setMaster("local")
sc = SparkContext(conf = conf)
sc.setLogLevel("ERROR")
sqlContext = SQLContext(sc)

spark = SparkSession.builder.getOrCreate()

df = spark.readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers", "instance-tram-1:9092")\
        .option("subscribe", "tram_positions_raw")\
        .option("failOnDataLoss", "false").\
        load()

table = df.selectExpr("CAST(key AS STRING)", "CAST(value as STRING)")

query = table.writeStream.format("console").outputMode("append").option("truncate", "false").start().awaitTermination()

print(query)
