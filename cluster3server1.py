from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'cluster3_subscriber1_topic', 
    bootstrap_servers='localhost:9092', 
    group_id='subscriber1_group'
)

for message in consumer:
    # Process the message for Subscriber 1
    print(f"Subscriber 1 received message: {message.value.decode('utf-8')}")