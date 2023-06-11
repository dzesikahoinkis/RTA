import argparse
import csv
import json
import sys
import time
from dateutil.parser import parse
from confluent_kafka import Producer
import socket
import requests

api_key = 'da3774ac0b6ae9601a50f00d0930cb4a'

def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg.value()), str(err)))
    else:
        print("Message produced: %s" % (str(msg.value())))


def get_weather_data(city):
    url = f'http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}'
    response = requests.get(url)
    data = response.json()
    return data


def main(city):

    conf = {'bootstrap.servers': "broker:9092",
            'client.id': socket.gethostname()}
    producer = Producer(conf)

    
    while True:

        try:
            weather_data = get_weather_data(city)
            jresult = json.dumps(weather_data)

            print(weather_data)

            producer.produce("test3", value=jresult, callback=acked)

            producer.flush()

        except TypeError:
            sys.exit()
            
        time.sleep(5) 


if __name__ == "__main__":
    main()
