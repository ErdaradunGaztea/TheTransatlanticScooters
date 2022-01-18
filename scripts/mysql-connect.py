import mysql.connector
from kafka import KafkaConsumer
import json

cnx = mysql.connector.connect(user='root', password='BigDataAnalytics',
        host='localhost:3309',
        database='weather_aggregates',
        unix_socket='/home/ubuntu/sockets/spring-cab-332810:europe-west1:serving-mysql')

cursor = cnx.cursor()

add_weather_data = ("REPLACE INTO weather_aggregates "
                    "(dt, temp, pressure, humidity, visibility, wind, cloud, rain, snow, pop)"
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);")
  
consumer = KafkaConsumer('weather-aggregates',
                         bootstrap_servers=['localhost:9092'])

weather_data = []

for message in consumer:
    record = json.loads(message.value.decode('utf8'))
    translated = ( 
        record.get("dt"),
        record.get("main.temp"),
        record.get("main.pressure"),
        record.get("main.humidity"),
        record.get("visibility"),
        record.get("wind.speed"),
        record.get("clouds.all"),
        record.get("rain.3h") or 0,
        record.get("snow.3h") or 0,
        record.get("pop")
    )
    # weather_data.append(translated)

    cursor.execute(add_weather_data, translated)

    cnx.commit()

cursor.close()
cnx.close()
