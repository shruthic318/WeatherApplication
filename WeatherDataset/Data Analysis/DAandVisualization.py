import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from scipy.stats import chi2_contingency
from sklearn.feature_selection import mutual_info_classif
from sklearn.preprocessing import LabelEncoder, StandardScaler
from unidecode import unidecode

# Provide a brief justification (3-4 lines) for why this dataset is suitable for weather-related analysis and predictions on a global scale.
#Justification: The Global weather repository consists of weather records captured for all countries inthe world, at a very granular level ie., on a daily basis.
# The dataset consists of many key features such as termperature celsius/farenheit,Country,precipitation, humidity, feels like celsius/farenheit,features about wind contributing to the prediction of the weather. The target label in this case would be condition_text, which captures the weather by classifying it in to categories such as 'Partly Cloudy','Patchy Rain nearby' etc..


#import the dataset from csv
df=pd.read_csv("C://Users//Admin//Downloads//archive//GlobalWeatherRepository.csv")
#print(df)

#remove the redundant columns from the dataset
selected_col=df.drop(['location_name','latitude','longitude','timezone','temperature_fahrenheit','wind_kph','pressure_in','precip_in','feels_like_fahrenheit','visibility_miles','gust_kph'],axis=1)
#print(selected_col.columns)

#Convert non-Asci characters to english
def ConvertToAsci(s):
    for char in s:
        if ord(char)>127:
            s=unidecode(s)
            break

    return unidecode(s)

#Convert the non-asci characters in country to English names
selected_col['country'] = selected_col['country'].apply(ConvertToAsci)
#COnvert to date to datetime values
selected_col['last_updated'] = pd.to_datetime(selected_col['last_updated'])
selected_col['sunrise_time'] = (pd.to_datetime(selected_col['sunrise'], format='%I:%M %p',errors='coerce')).dt.time
selected_col['sunset_time'] = pd.to_datetime(selected_col['sunset'], format='%I:%M %p',errors='coerce').dt.time
selected_col['moonrise_time'] = pd.to_datetime(selected_col['moonrise'], format='%I:%M %p',errors='coerce').dt.time
selected_col['moonset_time'] = pd.to_datetime(selected_col['moonset'], format='%I:%M %p',errors='coerce').dt.time


#Convert sunrise, sunset, moonrise and moonset data type to minutes
selected_col['sunrise_minutes'] = selected_col['sunrise_time'].apply(lambda x: x.hour * 60 + x.minute)
selected_col['sunset_minutes'] = selected_col['sunset_time'].apply(lambda x: x.hour * 60 + x.minute)
selected_col['moonrise_minutes'] = selected_col['moonrise_time'].apply(lambda x: x.hour * 60 + x.minute)
selected_col['moonset_minutes'] = selected_col['moonset_time'].apply(lambda x: x.hour * 60 + x.minute)

#drop the existing columns related to sunrise/set and moonrise/set
selected_col = selected_col.drop(['sunrise','sunset','moonrise','moonset','sunrise_time','sunset_time','moonrise_time','moonset_time'],axis=1)

#Data ANalytics and Visualization
#Top 5 hottest and coldest location globally
selected_country_maxtemp=selected_col.groupby('country')['temperature_celsius'].max().reset_index().rename(columns={'temperature_celsius':'Max_temp'})
selected_country_mintemp=selected_col.groupby('country')['temperature_celsius'].min().reset_index().rename(columns={'temperature_celsius':'Min_temp'})
df_hottest=selected_country_maxtemp.nlargest(5,'Max_temp')
df_coldest=selected_country_mintemp.nsmallest(5,'Min_temp')
#print(df_hottest)
#print(df_coldest)

#Monthly Average temperature by country
selected_col['last_updated_month']=selected_col['last_updated'].dt.month
monthly_average_temperature=selected_col.groupby(['country','last_updated_month'])['temperature_celsius'].mean().reset_index().rename(columns={'temperature_celsius':'Average_temp_celsius'})
#print(monthly_average_temperature.head(20))

#Group the data by region , year and compute the average maximum or minimum temperature, humidity or precipitaiton
#temperature: Average, Maximum, minimum
avgtemp_groupbycountry=selected_col.groupby('country')['temperature_celsius'].mean().reset_index().rename(columns={'temperature_celsius':'average_temperature'})
maxtemp_groupbycountry=selected_col.groupby('country')['temperature_celsius'].max().reset_index().rename(columns={'temperature_celsius':'max_temperature'})
mintemp_groupbycountry=selected_col.groupby('country')['temperature_celsius'].min().reset_index().rename(columns={'temperature_celsius':'min_temperature'})
#print(avgtemp_groupbycountry,maxtemp_groupbycountry,mintemp_groupbycountry)

#Monthly average/minimum/maximum humiditiy by country
selected_col['last_updated_month']=selected_col['last_updated'].dt.month
monthly_average_humidity=selected_col.groupby(['country','last_updated_month'])['humidity'].mean().reset_index().rename(columns={'humidity':'Average_humidity'})
monthly_max_humidity=selected_col.groupby(['country','last_updated_month'])['humidity'].max().reset_index().rename(columns={'humidity':'Average_humidity'})
monthly_min_humidity=selected_col.groupby(['country','last_updated_month'])['humidity'].min().reset_index().rename(columns={'humidity':'Average_humidity'})
#print(monthly_average_humidity, monthly_max_humidity,monthly_min_humidity)

#Histogram of temperatures
'''plt.hist(selected_col['temperature_celsius'],bins=15,range=[1.0,50.0])
plt.title('Temperature count')
plt.xlabel("temperature")
plt.ylabel("count of temperature")
'''#plt.show()

#line graph showing changes in temperature over a period of time, for a particular region
#extracting the temperature and last_updated data for the region 'India'
India_df=selected_col[selected_col['country']=='India'][['temperature_celsius','last_updated']]
plt.plot(India_df['last_updated'],India_df['temperature_celsius'],marker='o')
plt.title('Changes in temperature')
plt.xlabel("Date-time")
plt.ylabel("temperature")
plt.show()

