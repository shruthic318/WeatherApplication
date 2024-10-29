from pymongo import MongoClient
import pandas as pd

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['GlobalWeather_data']
collection = db['WeatherData']

# Load data from CSV
filepath = "C://Users//Admin//Downloads//OutputData//Weather_Forecast_Cleaned.csv"
weather_data = pd.read_csv(filepath)

# Convert DataFrame to dictionary
data_dict = weather_data.to_dict(orient='records')

# Insert data into MongoDB
collection.insert_many(data_dict)
print("Data inserted successfully!")

#print all the data in the collection
'''for doc in collection.find():
    print(doc)
'''


#to find the documents specific to a particular month
# filter the records by 10th month
filter_by_month = "10"
# Query to filter records by the specific month
query = {
    '$expr': {
        '$eq': [{ '$arrayElemAt': [{ '$split': ["$last_updated", "-"] }, 1]} , filter_by_month]
    }
}
# Find documents matching the query
matching_documents = collection.find(query)
# Print the matching records
'''for document in matching_documents:
    print(document)
'''
#top 3 records with highest temperature/lowest temperature
#(i)Top 3 country with Highest Temperature
# Aggregation pipeline to group by country and get the highest temperature
'''pipeline = [
    {
        '$group': {
            '_id': '$country',
            'max_temp': { '$max': '$temperature_celsius' },
            'doc': { '$first': '$$ROOT' }  
        }
    },
    {
        '$sort': { 'max_temp': -1 }
    },
    {
        '$limit': 3  # Adjust to get the top 3 or any other number
    }
]
# Execute the aggregation pipeline
results = collection.aggregate(pipeline)
# Print the results
print("Top 3 Countries with Highest Temperatures:")
for result in results:
    print(result)
'''
# (i)Top 3 country with Lowest Temperature
# Aggregation pipeline to group by country and get the lowest temperature
pipeline = [
    {
        '$group': {
            '_id': '$country',
            'min_temp': {'$min': '$temperature_celsius'},
            'doc': {'$first': '$$ROOT'}
        }
    },
    {
        '$sort': {'min_temp': 1}
    },
    {
        '$limit': 3  # Adjust to get the top 3 or any other number
    }
]
# Execute the aggregation pipeline
results = collection.aggregate(pipeline)
# Print the results
print("Top 3 Countries with Lowest Temperature:")
for result in results:
    print(result)