from time import sleep

from kafka import KafkaProducer
from kafka import KafkaConsumer



#producer = KafkaProducer(bootstrap_servers=['masternode:9092' ])

producer = KafkaProducer(bootstrap_servers=['masternode:9092'])

topic = "responses"

print(producer.send(topic, b'tessssssst message'))
sleep(1)
#
# consumer = KafkaConsumer('requests', bootstrap_servers=[ 'masternode:9092' ])
#
# for message in consumer:
#  print(message)