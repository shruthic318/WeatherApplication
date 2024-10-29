import pandas as pd
from kafka import KafkaConsumer
import json
import time
from jsonpath_ng import jsonpath, parse
import logging

#Create Consumer Object
#offset - messages are labelled uniquely as offset
#auto_offset_reset - Every time a consumer object is created, setting 'earliest'- will pick the message from teh start of th message;
#   if 'latest' - it will pick the message from the offset, where it had left ie., only new messges will be picked
#enable_auto_commit - this will commit the offsets (could be multiple records of data) at regular intervals
#value_deserializer expects callable function, which it can use to process each message it concumes from kafka
consumer=KafkaConsumer("global_weather",bootstrap_servers="localhost:9092",
                       auto_offset_reset='earliest',enable_auto_commit=True,
                       group_id='globalweather-group',
                       value_deserializer=lambda x:json.loads(x.decode('utf-8'))
                       )


weatherDict={}
weather_columns=['country','last_updated','temperature_celsius','condition_text','wind_mph','wind_degree','wind_direction',
                   'pressure_mb','precip_mm','humidity','cloud','feels_like_celsius','visibility_km','uv_index','gust_mph',
                 'air_quality_carbon_monoxide','air_quality_ozone','air_quality_nitrogen_dioxide','air_quality_sulphur_dioxide',
                 'air_quality_PM2.5','air_quality_PM10','air_quality_us-epa-index','air_quality_gb-defra-index','moon_phase','moon_illumination',
                 'sunrise_minutes','sunset_minutes','moonrise_minutes','moonset_minutes']

col_data={col:[] for col in weather_columns }

#function to get data based on jsonpath
def getJsonElement(data,parameterName):

    jsonpath_expr=parse(parameterName)
    matches=jsonpath_expr.find(data)
    for match in matches:
        return match.value


#Consume messages
try:
    start_time = time.time()
    while time.time() - start_time < 60:
        for message in consumer:
            if time.time() - start_time >= 60:
                break
            for col in weather_columns:
                if(len(col_data[col])<61):
                    col_data[col].append(getJsonElement(message.value,col))
                else:
                    break

            #process_data(message.value)
except KeyboardInterrupt:
    print('Stopping consumer')

def createDataframe(dict):
    df=pd.DataFrame(dict)
    return df

#print the data frame
print(createDataframe(col_data))
weatherdf=createDataframe(col_data)

#Close the consumer
consumer.close()