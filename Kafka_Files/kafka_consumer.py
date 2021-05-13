from kafka import KafkaConsumer
KAFKA_TOPIC = 'coinrank'


consumer = KafkaConsumer(KAFKA_TOPIC, bootstrap_servers=['localhost:9092'], auto_offset_reset='earliest',
                         enable_auto_commit=True, group_id='my-group',auto_commit_interval_ms=1000)
for message in consumer:
    message = message.value
    print(message.decode('utf-8'))
