from kafka import KafkaConsumer
import json

brokers = ["localhost:9091", "localhost:9092", "localhost:9093"]
topicName = "trips"
consumer = KafkaConsumer(topicName, bootstrap_servers=brokers)

for message in consumer:
    #['','VendorID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'passenger_count', 'trip_distance', 'RatecodeID', 'store_and_fwd_flag', 'PULocationID', 'DOLocationID', 'payment_type', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'improvement_surcharge', 'total_amount', 'congestion_surcharge', 'airport_fee']
    row = json.loads(message.value.decode())
    if float(row[11]) > 10:
        #print("--over 10$--")
        print(f"{row[10]} - {row[11]}")