import pandas as pd
import mysql.connector

# Connect to MySQL database
conn = mysql.connector.connect(
    host='127.0.0.1',
    user='root',
    password='Shruthi318',
    database='GlobalWeatherDB'
)

# Create a cursor object
cursor = conn.cursor()

# Import dataset into Pandas dataframe
filepath = "C://Users//Admin//Downloads//OutputData//Weather_Forecast_Cleaned.csv"
weather_data = pd.read_csv(filepath)


# Insert data into MySQL table
for i, row in weather_data.iterrows():
    try:
        insert_query = '''
        INSERT INTO WEATHERDATA (
            ID, COUNTRY, LAST_UPDATED, TEMPERATURE_CELSIUS, CONDITION_TEXT, WIND_MPH, WIND_DEGREE, WIND_DIRECTION,
            PRESSURE_MB, PRECIP_MM, HUMIDITY, CLOUD, FEELS_LIKE_CELSIUS, VISIBILITY_KM, UV_INDEX, GUST_MPH,
            AIR_QUALITY_CM, AIR_QUALITY_OZONE, AIR_QUALITY_ND, AIR_QUALITY_SD, AIR_QUALITY_PM_2_5, AIR_QUALITY_PM_10,
            AIR_QUALITY_US_EPA, AIR_QUALITY_US_GB, MOON_PHASE, MOON_ILLUMINATION, SUNRISE_MINUTES, SUNSET_MINUTES,
            MOONRISE_MINUTES, MOONSET_MINUTES
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        '''
        #data_tuple = tuple(row)
        #print(data_tuple)  # Print each row for debugging
        #cursor.execute(insert_query, data_tuple)
    except mysql.connector.Error as err:
        print(f"Error: {err}")



#Top 5 locations with highest temperature
MAX_TEMP_COUNTRY_QUERY="SELECT * FROM (SELECT COUNTRY,MAX(TEMPERATURE_CELSIUS) MAX_TEMP FROM WEATHERDATA GROUP BY COUNTRY) MAXTEMP_COUNTRY ORDER BY MAX_TEMP DESC LIMIT 5;"
print(cursor.execute(MAX_TEMP_COUNTRY_QUERY))
results=cursor.fetchall()
for row in results:
    print(row)
print('**********')
#Top 5 locations with  lowest precipitation
LOW_TEMP_COUNTRY_QUERY="SELECT * FROM (SELECT COUNTRY,MIN(TEMPERATURE_CELSIUS) MIN_TEMP FROM WEATHERDATA GROUP BY COUNTRY) LOWTEMP_COUNTRY ORDER BY MIN_TEMP ASC LIMIT 5;"
cursor.execute(LOW_TEMP_COUNTRY_QUERY)
results=cursor.fetchall()
for row in results:
    print(row)

#Filter the weather data by temperature=35 or precipiation =100, for the data '2024-05-16'
QUERY3="SELECT * FROM WEATHERDATA WHERE (TEMPERATURE_CELSIUS=35.0 OR PRECIP_MM=100 ) AND LAST_UPDATED LIKE '2024-05-16%'"
cursor.execute(QUERY3)
results=cursor.fetchall()
for row in results:
    print(row)

#Group the weather data by country and get the average temperature and total precipitation
QUERY4="SELECT COUNTRY,AVG(TEMPERATURE_CELSIUS),SUM(PRECIP_MM) FROM WEATHERDATA GROUP BY COUNTRY"
cursor.execute(QUERY4)
results=cursor.fetchall()
for row in results:
    print(row)


conn.commit()

# Close the cursor and connection
cursor.close()
conn.close()

print("Data imported successfully!")