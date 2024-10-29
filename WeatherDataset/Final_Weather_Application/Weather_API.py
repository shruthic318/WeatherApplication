# app.py
import os

from flask import Flask, jsonify, request
import pandas as pd
from kafka import KafkaProducer, KafkaConsumer
import json
import threading

app = Flask(__name__)

# Load cleaned data
filepath = os.path.join(os.path.dirname(__file__), 'Weather_Forecast_Cleaned.csv')  # Update this path accordingly
weather_data = pd.read_csv(filepath)

# Kafka Producer setup
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Function to stream data to Kafka
def stream_data_to_kafka():

    for _, row in weather_data.iterrows():
        record = row.to_dict()
        producer.send('global_weather', value=record)
        print(f"Sent: {record}")
    producer.flush()

# Start streaming data to Kafka in a separate thread
def start_streaming():
    threading.Thread(target=stream_data_to_kafka).start()

# API endpoint to get cleaned weather data
@app.route('/weather', methods=['GET'])
def get_weather():
    data = weather_data.to_dict(orient='records')
    return jsonify(data)

# API endpoint to start streaming
@app.route('/start_stream', methods=['GET'])
def start_stream():
    start_streaming()
    return jsonify({"status": "Streaming started!"})

# API endpoint for live updates (Kafka Consumer)
@app.route('/live_updates', methods=['GET'])
def live_updates():
    consumer = KafkaConsumer(
        'global_weather',
        bootstrap_servers='localhost:9092',
        #auto_offset_reset='earliest',
        #enable_auto_commit=False,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    updates = []
    for message in consumer:
        # Log each message to inspect its structure
        print(f"Received message: {message.value}")  # Add this line to debug
        updates.append(message.value)

        # Limit the number of updates returned
        if len(updates) >= 30:
            break
    return jsonify(updates)

if __name__ == '__main__':
    app.run(debug=True)
