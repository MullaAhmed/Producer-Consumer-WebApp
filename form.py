import requests
from kafka import KafkaProducer
import json

# Reading From API
"""
This part is mimiciking the incoming form input from the user
"""
api_url = "http://127.0.0.1:5000/"
api_response = requests.get(api_url)

hostname="localhost:9092" #Karafka server URL
topic_name="test1"        #Topic name

producer = KafkaProducer(
 bootstrap_servers=hostname,
 value_serializer=lambda v: json.dumps(v).encode('ascii'),
 key_serializer=lambda v: json.dumps(v).encode('ascii')
)

producer.send(
    topic_name,
    key={"id":1},               #Key will be unique for each form 
    value=api_response.json()
)