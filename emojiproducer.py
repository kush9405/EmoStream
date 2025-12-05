import requests
import json
import time
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

server_url = 'http://localhost:5000/send_emoji'
emojis = ['😄', '😊', '😂', '😃', '😁','🏀', '⚽', '🏆', '⚾', '🏈','🎉', '🥳', '🎊', '🎈','👍','👎']
#emojis = ['👎']
emoji_count = 0
lock = threading.Lock()

def generate_and_send_emojis(client_id):
    global emoji_count
    i=0
    while True:
        i+=1
        try:
            emoji_data = {
                "user_id": client_id,
                "emoji_type": random.choice(emojis),
                "timestamp": time.time(),
                "count":i
            }
            
            response = requests.post(server_url, json=emoji_data)
            if response.status_code != 200:
                print("Failed to send emoji:", response.json())
            with lock:
                emoji_count += 1
            time.sleep(0.05)  # Adjust the sleep time to achieve the desired rate
        except Exception as e:
            # print(f"Error: {e}")
            pass

def print_emoji_count():
    global emoji_count
    
    while True:
        time.sleep(1)  # Sleep for 1 second
        with lock:
            print(f"Emojis generated in the last second: {emoji_count}")
            emoji_count = 0

if __name__ == '__main__':  # Corrected the error here, it should be '__name__' and '__main__'
    client_id = input("Enter your client ID: ")
    num_threads = 125  # Number of threads to use for generating emojis
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [executor.submit(generate_and_send_emojis, client_id) for _ in range(num_threads)]
        threading.Thread(target=print_emoji_count, daemon=True).start()
        for future in as_completed(futures):
            future.result()


