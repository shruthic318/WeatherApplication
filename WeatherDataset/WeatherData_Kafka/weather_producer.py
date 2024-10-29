from datetime import datetime
import pandas as pd
import time
from kafka import KafkaProducer
import json
import logging

#Set up logging
logging.basicConfig(level=logging.INFO)

#import dataset into Pandas dataframe
filepath="/mnt/c/Users/Admin/Downloads/OutputData/Weather_Forecast_Cleaned.csv"
#filepath="C://Users//Admin//Downloads//OutputData//Weather_ForeCast_Cleaned.csv"
weather_data=pd.read_csv(filepath)
print(weather_data.columns)

#create an instance of producer to stream data to kafka
producer=KafkaProducer(bootstrap_servers="localhost:9092",value_serializer=lambda v:json.dumps(v).encode('utf-8'))

#function to iterate the rows in the dataset
#def iteratetheStreamingData(data,delay,duration):
def iteratetheStreamingData(data,delay):
    start_time = time.time()
    try:
        for index, row in data.iterrows():
            try:
                #send the paraters topic and row converted to dictionary
                record=row.to_dict()
                #record['timestamp']=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                producer.send('global_weather',value=record)
                logging.info("Sent Record",record)

                time.sleep(delay)
                yield record
                print(record)
            except Exception as e:
                logging.error("Error sending record:",e)
                return 0
    finally:
        producer.close()

#stream the data
#iteratetheStreamingData(weather_data,1.0,60)

#Close the producer
#producer.close()



