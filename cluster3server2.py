from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'cluster3_subscriber2_topic', 
    bootstrap_servers='localhost:9092', 
    group_id='subscriber2_group'
)

for message in consumer:
    # Process the message for Subscriber 2
    print(f"Subscriber 2 received message: {message.value.decode('utf-8')}")