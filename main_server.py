from flask import Flask, request, jsonify, render_template_string, url_for
from datetime import datetime
from kafka import KafkaProducer
import json
from concurrent.futures import ThreadPoolExecutor

app = Flask(__name__)

# In-memory user storage for simplicity
users = {
    "user123": {"user_id": "user123", "password": "password123"},
    "user1": {"user_id": "user1", "password": "password1"},
    "user2": {"user_id": "user2", "password": "password2"},
    "user3": {"user_id": "user3", "password": "password3"},
    "user4": {"user_id": "user4", "password": "password4"},
    "kush": {"user_id": "kush", "password": "password4"}
}

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    linger_ms=500
)

# ThreadPoolExecutor for asynchronous Kafka writing
executor = ThreadPoolExecutor(max_workers=5)

# Helper function to classify emoji
def classify_emoji(emj):
    happy_emojis = {'😄', '😊', '😂', '😃', '😁'}
    sad_emojis = {'😢', '😞', '😔', '😕', '😟'}
    sports_emojis = {'🏀', '⚽', '🏆', '⚾', '🏈'}
    celebration_emojis = {'🎉', '🥳', '🎊', '🎈'}
    thumbs_up_emojis = {'👍'}
    thumbs_down_emojis = {'👎'}

    if emj in happy_emojis:
        return "happy"
    elif emj in sad_emojis:
        return "sad"
    elif emj in sports_emojis:
        return "sports"
    elif emj in celebration_emojis:
        return "celebration"
    elif emj in thumbs_up_emojis:
        return "positive"
    elif emj in thumbs_down_emojis:
        return "negative"
    else:
        return "unknown"

# Serve the HTML form with emojis and a video
@app.route('/')
def index():
    emojis = ['😄', '😊', '😂', '😢', '😞', '🏀', '⚽', '🎉', '🥳', '👍', '👎']
    video_url = url_for('static', filename='welcome-letter-banner-poster-vibrant-background_667085-132.avif')
    background_url = url_for('static', filename='welcome-letter-banner-poster-vibrant-background_667085-132.avif')
    return render_template_string('''
    <!DOCTYPE html>
    <html>
    <head>
        <title>Emoji Submission</title>
        <style>
            body { 
                display: flex; 
                flex-direction: column; 
                justify-content: center; 
                align-items: center; 
                height: 100vh; 
                margin: 0; 
                font-family: Arial, sans-serif;
                background-image: url('{{ background_url }}');
                background-size: cover;
                background-position: center;
                color: white;
            }
            .header {
                font-size: 2em; 
                font-weight: bold; 
                background: linear-gradient(90deg, red, orange, yellow); 
                -webkit-background-clip: text; 
                color: transparent; 
                position: fixed; 
                top: 0; 
                left: 0; 
                margin: 10px; 
                padding: 10px;
            }
            .video-container { 
                display: flex; 
                justify-content: center; 
                margin-bottom: 20px; 
            }
            .emoji-container { 
                display: flex; 
                justify-content: center; 
                flex-wrap: wrap; 
            }
            .emoji { 
                font-size: 50px; 
                cursor: pointer; 
                margin: 5px; 
            }
            .floating-emoji {
                position: absolute;
                animation: floatUp 2s forwards;
                font-size: 50px;
            }
            @keyframes floatUp {
                0% { opacity: 1; transform: translateY(0); }
                100% { opacity: 0; transform: translateY(-600px); }
            }
            .emoji-input-container {
                margin: 20px 0;
                text-align: center;
            }
            input[type="text"] {
                width: 80%;
                padding: 10px;
                font-size: 18px;
                text-align: center;
            }
        </style>
        <script>
            function sendEmoji(emoji) {
                const emojiInput = document.getElementById("emojiInput");
                emojiInput.value += emoji;

                // Show floating emoji effect
                const floatingEmoji = document.createElement("span");
                floatingEmoji.classList.add("floating-emoji");
                floatingEmoji.textContent = emoji;
                document.body.appendChild(floatingEmoji);

                // Position it over the clicked emoji
                floatingEmoji.style.left = event.clientX + "px";
                floatingEmoji.style.top = event.clientY + "px";

                // Remove the emoji after animation completes
                floatingEmoji.addEventListener("animationend", () => {
                    floatingEmoji.remove();
                });

                // Send emoji data to backend
                fetch('/send_emoji', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                    },
                    body: JSON.stringify({
                        user_id: 'user123',
                        emoji_type: emoji,
                        timestamp: new Date().toISOString()
                    })
                })
                .then(response => response.json())
                .catch(error => console.error('Error:', error));
            }

            window.onload = function() {
                fetchDataFromServer();
            };

            function fetchDataFromServer() {
                fetch('/receive_data', {
                    method: 'POST',
                })
                .then(response => {
                    if (!response.ok) {
                        throw new Error(`HTTP error! Status: ${response.status}`);
                    }
                    return response.json();
                })
                .then(data => {
                    console.log('Fetched JSON response:', data);
                    const receivedDataDiv = document.getElementById('receivedData');
                    receivedDataDiv.innerHTML = `<strong>Fetched Data:</strong> ${JSON.stringify(data)}`;

                    const floatingData = document.createElement("span");
                    floatingData.classList.add("floating-emoji");
                    floatingData.textContent = '📄';
                    document.body.appendChild(floatingData);
                    floatingData.style.left = (window.innerWidth / 2) + "px";
                    floatingData.style.top = (window.innerHeight / 2) + "px";
                    floatingData.addEventListener("animationend", () => {
                        floatingData.remove();
                    });
                })
                .catch(error => console.error('Error:', error));
            }
        </script>
    </head>
    <body>
    <div class="header">
        EmoStream
    </div>
        <div class="video-container">
            <video width="600" autoplay loop muted>
                <source src="{{ video_url }}" type="video/mp4">
                Your browser does not support the video tag.
            </video>
        </div>
        
        <div class="emoji-input-container">
            <input type="text" id="emojiInput" placeholder="Your emojis will appear here..."  />
        </div>

        <div id="emojiContainer" class="emoji-container">
            {% for emoji in emojis %}
                <span class="emoji" onclick="sendEmoji('{{ emoji }}')">{{ emoji }}</span>
            {% endfor %}
        </div>

        <div id="receivedData" style="margin-top: 20px; color: white;"></div>
    </body>
    </html>
    ''', emojis=emojis, video_url=video_url, background_url=background_url)

# Handle emoji sending
@app.route('/send_emoji', methods=['POST'])
def send_emoji():
    data = request.get_json()
    user_id = data.get('user_id')
    emoji_input = data.get('emoji_type')
    timestamp = data.get('timestamp')

    if user_id not in users:
        return jsonify({"message": "User not found"}), 403

    emoji_type = classify_emoji(emoji_input)
    with open("emoji_log.txt", "a") as f:
        f.write(f"{user_id},{emoji_type},{timestamp}\n")

    message = {
        "user_id": user_id,
        "emoji_type": emoji_type,
        "timestamp": timestamp
    }
    print(message)
    executor.submit(producer.send, 'emoji_topic', value=message)

    return jsonify({
        "user_id": user_id,
        "message": "Emoji received",
        "emoji_type": emoji_type,
        "timestamp": timestamp
    }), 200

@app.route('/receive_data', methods=['POST'])
def receive_data():
    data = request.get_json()
    print(f"Received data: {data['data']}")  # Python logging for the backend
    return jsonify({'message': 'Data received successfully', 'received_data': data}), 200

if __name__ == '__main__':
    app.run(debug=True)
