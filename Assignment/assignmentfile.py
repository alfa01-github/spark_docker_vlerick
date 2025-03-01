# SECTION ONE OF THE ASSIGNMENT
import os

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import LabelEncoder, StandardScaler, MinMaxScaler
from sklearn.impute import SimpleImputer

BUCKET = "dmacademy-course-assets"
KEYafter = "vlerick/after_release.csv"
KEYpre = "vlerick/pre_release.csv"

if 'AWS_SECRET_ACCESS_KEY' in os.environ:
    print("credentials are present in the environment")
    config = {
        "spark.jars.packages": "org.apache.hadoop:hadoop-aws:3.3.1",
        "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    }
else:
    print("you have no credentials in the environment")
    config = {
        "spark.jars.packages": "org.apache.hadoop:hadoop-aws:3.3.1",
        "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.InstanceProfileCredentialsProvider",
    }
    

conf = SparkConf().setAll(config.items())
spark = SparkSession.builder.config(conf=conf).getOrCreate()

dfpre = spark.read.csv(f"s3a://{BUCKET}/{KEYpre}", header=True)
dfpre.show()
dfafter = spark.read.csv(f"s3a://{BUCKET}/{KEYafter}", header=True)
dfafter.show()

# SECTION TWO OF THE ASSIGNMENT

import pandas as pd 

pre = dfpre.toPandas()
after = dfafter.toPandas()

# SECTION THREE OF THE ASSIGNMENT
# # IMDB Case
df = pd.merge(pre, after[['movie_title', 'imdb_score']], how='inner', on='movie_title') 
df.head()


#Removing the null values from the dataset, since there aren't a lot of null values
df.dropna(how = 'any',axis = 0,inplace = True)

#Removing duplicates, 22 rows ae being removed
df.drop_duplicates(inplace = True)
df.shape

df.drop('actor_2_facebook_likes', inplace = True, axis = 1)
df.drop('actor_3_facebook_likes', inplace = True, axis = 1)
df.drop('actor_1_facebook_likes', inplace = True, axis = 1)

# ## Grouping categorical variables and changing them into dummies 
value_counts = df["language"].value_counts()

val = value_counts[:1].index
print (val)
df['language'] = df.language.where(df.language.isin(val), 'other')

le = LabelEncoder()
df['language'] = le.fit_transform(df['language'])

# ### Countries
#Value count for the countries, most movies being from the USA followed by a list of other countries
value_counts = df["country"].value_counts()

#Selecting the USA grouping rest of countries
val = value_counts[:3].index
print (val)
df['country'] = df.country.where(df.country.isin(val), 'other')
df["country"].value_counts()

df = pd.concat([df, pd.get_dummies(df['country'])], axis=1)
df.drop('country', axis=1, inplace=True) #dropping original country column
print(df)

# ### Content rating
#Value count for the content rating of the movies
value_counts = df["content_rating"].value_counts()


#Selecting R, PG-13 and combing other ratings 
val = value_counts[:3].index
print (val)
df['content_rating'] = df.content_rating.where(df.content_rating.isin(val), 'other')


#Successfully grouped content rating into 3 categories
df["content_rating"].value_counts()


#Generating binary values using get_dummies
df = pd.concat([df, pd.get_dummies(df['content_rating'])], axis=1)
df.drop('content_rating', axis=1, inplace=True) #dropping original content rating column


# ## Split Genres on delimiter and add as dummies
value_counts = df["genres"].value_counts()
print(value_counts)

#turn the categorical variable genres into dummies by spliting on delimiters and dropping original genres column
df = pd.concat([df, df['genres'].str.get_dummies('|')], axis=1)
df.drop('genres', axis=1, inplace=True)
print(df)

#Dropping the irrelevant variables and defining dependent and independent variables 
#extract target variable
x = df.drop(columns = ["imdb_score","director_name","actor_2_name","actor_1_name","actor_3_name","movie_title"])
y = df["imdb_score"] #Being the target variable
print(x)
print(y)

#Randomly splitting into a training and validation sample
seed = 123 
x_train, x_val, y_train, y_val = train_test_split(x,y,test_size=0.25, random_state = seed)

rfreg = RandomForestRegressor(max_depth=10, min_samples_leaf =1, random_state=0).fit(x_train, y_train)

#Predict regression forest
array_pred = np.round(rfreg.predict(x_val),0)
y_pred = pd.DataFrame({"y_pred": array_pred},index=x_val.index) #index must be same as original database
val_pred = pd.concat([y_val,y_pred,x_val],axis=1)
print(val_pred)

# SECTION FOUR OF THE ASSIGNMENT
df = spark.createDataFrame(val_pred)

# SECTION FIVE OF THE ASSIGNMENT
df.write.json(f"s3a://dmacademy-course-assets/vlerick/allison", mode="overwrite")

# SECTION SIX OF THE ASSIGNMENT


