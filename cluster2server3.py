from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'cluster2_subscriber3_topic', 
    bootstrap_servers='localhost:9092', 
    group_id='subscriber3_group'
)

for message in consumer:
    # Process the message for Subscriber 3
    print(f"Subscriber 3 received message: {message.value.decode('utf-8')}")