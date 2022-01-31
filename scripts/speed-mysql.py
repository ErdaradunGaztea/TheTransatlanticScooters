import mysql.connector
from kafka import KafkaConsumer
import json
from datetime import datetime

cnx = mysql.connector.connect(user='root', password='BigDataAnalytics',
                host='localhost:3309',
                database='velocity',
                unix_socket='/home/ubuntu/sockets/spring-cab-332810:europe-west1:serving-mysql')

cursor = cnx.cursor()

add_weather_data = ("REPLACE INTO Velocity "
                    "(dt, line, brigade, velocity)"
                    "VALUES (%s, %s, %s, %s);")
  
consumer = KafkaConsumer('velocity',
                         bootstrap_servers=['localhost:9092'])

weather_data = []

for message in consumer:
    record = json.loads(message.value.decode('utf8'))
    if float(record.get("velocity")) > 100:
        continue
    translated = (
        int(datetime.strptime(record.get("window").get("start"), '%Y-%m-%dT%H:%M:%S.%fZ').timestamp()),
        record.get("line"),
        record.get("Brigade"),
        record.get("velocity")    
    )
    # weather_data.append(translated)

    cursor.execute(add_weather_data, translated)

    cnx.commit()

cursor.close()
cnx.close()
