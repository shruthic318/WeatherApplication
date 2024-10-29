import os

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

#Display the number of row against columns
print(df.shape)
#Display the data types of the columns
print(df.info())

#finding the missing values
#Output: There are no missing records in the dataset
#print(df.columns)
'''print(df.isnull().sum())'''



#find the duplicate rows
#Output: There are no duplicate rows in the dataset
'''print(np.where(df.duplicated()==True))'''

#remove the redundant columns from the dataset
selected_col=df.drop(['location_name','latitude','longitude','timezone','temperature_fahrenheit','wind_kph','pressure_in','precip_in','feels_like_fahrenheit','visibility_miles','gust_kph'],axis=1)
print(selected_col.columns)

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

#fill the missing values in moonrise and moonset with 0, for the ones which have no moonrise/moonset data
selected_col['moonrise_minutes']=selected_col['moonrise_minutes'].fillna(0.0)
selected_col['moonset_minutes']=selected_col['moonset_minutes'].fillna(0.0)

#drop the existing columns related to sunrise/set and moonrise/set
selected_col = selected_col.drop(['sunrise_time','sunset_time','moonrise_time','moonset_time','last_updated_epoch','sunrise_time','sunset_time','moonrise_time','moonset_time','sunrise','sunset','moonrise','moonset'],axis=1)
print("selected_col",selected_col.columns)

#Separate the features from the target variable
feature=selected_col.drop('condition_text',axis=1)
Y=selected_col['condition_text']

#Move the cleaned data to csv file
#os.path.join(os.path.dirname(__file__), 'Output/Cleaned_Data.csv')
OutputFolderpath = os.path.join(os.path.dirname(__file__), 'Output/Cleaned_Data.csv')
selected_col.to_csv(OutputFolderpath)

#Displaying Key statistics
print(selected_col.describe())




