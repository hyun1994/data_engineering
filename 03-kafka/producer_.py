from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

producer.send("first-topic", b"hellow world from python")
producer.flush()