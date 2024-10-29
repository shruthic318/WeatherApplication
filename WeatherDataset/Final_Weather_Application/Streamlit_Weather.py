# streamlit_app.py
import time

import streamlit as st
import pandas as pd
import requests
import json
import matplotlib.pyplot as plt

# API URL
BASE_URL = 'http://13.50.100.253:5000'

# Streamlit App Title
st.title("Global Weather Dashboard")

# Data Dashboard Section
st.header("Data Dashboard")
st.write("This section displays the cleaned global weather dataset.")

# Fetching the cleaned weather data from Flask API
weather_data = requests.get(f"{BASE_URL}/weather").json()
df = pd.DataFrame(weather_data)

# Filter Options
region = st.sidebar.selectbox("Select Country:", df['country'].unique())
date = st.date_input("Select Date:")
df['last_updated'] = pd.to_datetime(df['last_updated'], format='%Y-%m-%d %H:%M:%S')
unique_dates = df['last_updated'].dt.date.unique()
date_filter = st.sidebar.selectbox('Date', unique_dates)
filtered_df = df[(df['last_updated'].dt.date == pd.to_datetime(date_filter).date()) & (df['country'] == region)]

st.write("Filtered Data:")
st.dataframe(filtered_df)
live_updates=[]
# Fetch live updates from Flask API

#**************************
# Start Streaming automatically
live_updates=[]
start_stream_url = f"{BASE_URL}/start_stream"
start_response = requests.get(start_stream_url)
if start_response.status_code == 200:
    st.success("Streaming started successfully!")

placeholder=st.empty()

# Optional: Plotting live updates (assuming updates contain numeric fields)
st.subheader("Real-Time Plot")
def plot_liveUpdates(live_updates):

    if live_updates:
        fig, ax = plt.subplots()
        ax.plot([update['last_updated'].split('-')[1] for update in live_updates], [update['temperature_celsius'] for update in live_updates], marker='o')
        ax.set_xlabel('Month-2024')
        ax.set_ylabel('temperature_celsius')
        ax.set_title('Live Temperature Updates')
        placeholder.pyplot(fig)


# Function to fetch and display live updates
def fetch_data():
    stream_url = f"{BASE_URL}/live_updates"
    while True:
        new_updates = requests.get(stream_url).json()
        if new_updates:
            live_updates.extend(new_updates)
            print(live_updates[1])
            plot_liveUpdates(live_updates)
            #latest_update_time = max(update['last_updated'] for update in live_updates)
            st.write("Latest Updates:")
            for update in new_updates:
                st.write(update)

        time.sleep(3)
    # Execute the function to fetch live updates
fetch_data()

#**********************

