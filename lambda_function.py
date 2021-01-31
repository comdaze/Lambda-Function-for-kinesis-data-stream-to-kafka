from kafka import KafkaProducer
import base64

def lambda_handler(event, context):
    producer = KafkaProducer(bootstrap_servers="b-2.demo-cluster-1.riia5x.c4.kafka.cn-north-1.amazonaws.com.cn:9092,b-1.demo-cluster-1.riia5x.c4.kafka.cn-north-1.amazonaws.com.cn:9092")
    print(producer.bootstrap_connected())
   
    for record in event['Records']:
       #Kinesis data is base64 encoded so decode here
       payload=base64.b64decode(record["kinesis"]["data"])
       print("Decoded payload: " + str(payload))
       producer.send("mytopic",payload)
       print(producer.send)
    return ("Messages Sent to Kafka Topic")
       