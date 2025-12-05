from kafka import KafkaConsumer
import requests
consumer = KafkaConsumer(
    'cluster3_subscriber3_topic', 
    bootstrap_servers='localhost:9092', 
    group_id='subscriber3_group'
)

# Define the API endpoint
api_endpoint = 'http://localhost:5000/receive_data'

def consume_messages():
    for message in consumer:
        # Process the message for Subscriber 3
        data = message.value.decode('utf-8')
        print(f"Subscriber 3 received message: {data}")
        
        # Send the data to the API endpoint
        response = requests.post(api_endpoint, json={'data': data})
        
        # Check the response status
        if response.status_code == 200:
            print("Data sent successfully to the API endpoint")
        else:
            print(f"Failed to send data to the API endpoint: {response.status_code}")

if __name__ == '__main__':
    import threading
    # Start the Flask app in a separate thread
    
    # Start consuming messages
    consume_messages()
