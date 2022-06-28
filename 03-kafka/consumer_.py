from kafka import KafkaConsumer

consumer = KafkaConsumer("first-topic", bootstrap-server=["localhost:9092"])

for messages in consumer:
    print(messages)
    