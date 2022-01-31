import mysql.connector
from kafka import KafkaConsumer
import json
from datetime import datetime

cnx = mysql.connector.connect(user='root', password='BigDataAnalytics',
                host='localhost:3309',
                database='predictions',
                unix_socket='/home/ubuntu/sockets/spring-cab-332810:europe-west1:serving-mysql')

cursor = cnx.cursor()

add_weather_data = ("REPLACE INTO predictions "
                    "(dt, line, prediction)"
                    "VALUES (%s, %s, %s);")
  
consumer = KafkaConsumer('predictions',
                         bootstrap_servers=['localhost:9092'])

weather_data = []

for message in consumer:
    record = json.loads(message.value.decode('utf8'))
    translated = (
        record.get("dt"),
        record.get("line"),
        record.get("prediction")    
    )
    # weather_data.append(translated)

    cursor.execute(add_weather_data, translated)

    cnx.commit()

cursor.close()
cnx.close()
