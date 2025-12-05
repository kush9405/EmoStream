from kafka import KafkaConsumer, KafkaProducer
import json

# Initialize Kafka consumer
consumer = KafkaConsumer(
    'main_publisher_topic',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='main_publisher_group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Initialize Kafka producers for each cluster
producer_cluster1 = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

producer_cluster2 = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

producer_cluster3 = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# Forward messages to the Kafka clusters
for message in consumer:
    data = message.value
    print(data)
    producer_cluster1.send('forward_to_cluster1_topic', value=data)
    producer_cluster2.send('forward_to_cluster2_topic', value=data)
    producer_cluster3.send('forward_to_cluster3_topic', value=data)
