# SECTION ONE OF THE ASSIGNMENT
import os
print(os.environ["AWS_ACCESS_KEY_ID"])

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

config = {
    "spark.jars.packages": "org.apache.hadoop:hadoop-aws:3.3.1",
    "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
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

data = pre.merge(after, how = "inner", on = "movie_title")
data = data.drop(columns = ["director_name", "actor_1_name", "actor_2_name", "actor_3_name", "movie_title"])
datana = data[data.isna().any(axis=1)]
data[data.isnull().any(axis=1)]
data = data[data.isnull().sum(axis=1) < 5]
data = pd.DataFrame(data)
m = data.select_dtypes('Int64')
data[m.columns]= m.round().astype(float)
cat = data.select_dtypes(include = 'object').columns.tolist()
num = data.select_dtypes(include = 'float64').columns.tolist()
data.isna().sum()
# For the content ratings, I have decided to make the ones that have Nan values "Not Rated" so that they are not blank. According to a quick google search, Not Rated means that the film has not been submitted to the rating organisation to provide a grade. Because there are some Na's I don't want to provide them with any rating, so I will make them Not Rated
data['content_rating'] = data['content_rating'].fillna('Not Rated')
data
# For the categoricals, I have decided to impute with the most frequent. For language this is an easy explanation because the majority of the films are in English and from the US, so that makes sense to impute. 
cat_imputer = SimpleImputer(strategy = "most_frequent") 
data[cat] = cat_imputer.fit_transform(data[cat])
data[cat].isnull().sum()
num_imputer = SimpleImputer(strategy='median')
data[num] = num_imputer.fit_transform(data[num])
data[num].isnull().sum()
data.drop_duplicates(inplace = True)
print(data[data.duplicated() == True].shape[0])
data["total_profit"] = data['budget'].sub(data['gross'], axis = 0) 
data["profit_perc"] = (data["total_profit"]/data["gross"])*100
data.country.value_counts(normalize=True) * 100
# There are too many Countries for me to make dummies for all of them. I only want to keep the top 5.
# My film company is in the USA and Germany, but if producing in one of the other countries results in the highest revenue, maybe I will need to open a new office or film on site there!

# Make a new list of countries that I will want to remove
country_list = data["country"].unique().tolist()

# Remove the countries that I actually want to keep so I can delete the others
country_list.remove('USA')
country_list.remove('UK')
country_list.remove('France')
country_list.remove('Canada')
country_list.remove('Germany')

# Go over the list in a loop to remove all of the countries that I want to delete and change into "Others"
for country in country_list:
  data = data.replace(country, 'Country Other')

# Check to see if it worked
data.country.value_counts(normalize=True) * 100

# Create dummy columns for all of the countries
data = pd.concat([data, data['country'].str.get_dummies()], axis=1)

# Delete the original Country column
data = data.drop(['country'], axis = 1)

# Delete one of the dummy columns
data = data.drop(["Country Other"], axis =1)

# The genre column is all kinds of crazy, with all of the genre tags for each film stuck together in the same column
# I want to split them up and look at the impact of the genre on gross revenue, to make sure that the screenplay will succeed given the genre.

# Create dummy columns for all of the genres by splitting them into different columns with the delimiter "|"
data = pd.concat([data, data['genres'].str.get_dummies('|')], axis=1)

# Delete the original genres column
data = data.drop(['genres'], axis = 1)

# Delete one of the genre dummy columns; I have chosen Western after looking quickly at the low count of Western films (above)
data = data.drop(["Western"], axis =1)

# Here I can see that there are a number of Unrated films, which is the same as Not Rated. I will combine these two into "Not Rated"
data['content_rating'] = data['content_rating'].replace(['Unrated'], 'Not Rated')

# There are too many content ratings for me to make dummies for all of them. I only want to keep the top 4. I think this will have minimal impact into the rating already, so I will go ahead and make that change.
# My film company usually produces R and PG-13 movies, so these are the most important to me.

# Make a new list of countries that I will want to remove
rating_list = data["content_rating"].unique().tolist()

# Remove the countries that I actually want to keep so I can delete the others
rating_list.remove('R')
rating_list.remove('PG-13')
rating_list.remove('PG')
rating_list.remove('G')
rating_list.remove("Not Rated")

# Go over the list in a loop to remove all of the countries that I want to delete and change into "Others"
for rating in rating_list:
  data = data.replace(rating, 'Rating Other')

# Check to see if it worked
data.content_rating.value_counts(normalize=True) * 100

# Create dummy columns for all of the ratings
data = pd.concat([data, data['content_rating'].str.get_dummies()], axis=1)

# Delete the original Country column
data = data.drop(['content_rating'], axis = 1)

# Delete one of the dummy columns
data = data.drop(["Rating Other"], axis =1)

data.language.value_counts(normalize=True) * 100

# There are too many languages for me to make dummies for all of them. I only want to keep the top 3. I think this will have minimal impact into the language already, so I will go ahead and make that change.
# My film company typically produces films in English, despite attracting global audiences. I will keep the other top languages in case this does impact the gross revenue!

# Make a new list of languages that I will want to remove
lang_list = data["language"].unique().tolist()

# Remove the languages that I actually want to keep so I can delete the others
lang_list.remove('English')
lang_list.remove('French')
lang_list.remove('Spanish')

# Go over the list in a loop to remove all of the countries that I want to delete and change into "Others"
for language in lang_list:
  data = data.replace(language, 'Language Other')

# Check to see if it worked
data.language.value_counts(normalize=True) * 100

# Create dummy columns for all of the languages
data = pd.concat([data, data['language'].str.get_dummies()], axis=1)

# Delete the original language column
data = data.drop(['language'], axis = 1)

# Delete one of the dummy columns
data = data.drop(["Language Other"], axis =1)

# Let's standardize our numerical data (used in further predictive techniques)!
scaler = StandardScaler()

data_scaled = data.copy()
data_scaled[num] = scaler.fit_transform(data_scaled[num])
data_scaled.head()

from sklearn.model_selection import train_test_split

# Note: I have removed facebook likes from actor 1, actor 2, and actor 3 in the pre_data dataframe below. 

pre_data = pd.DataFrame(columns = ['duration', 'director_facebook_likes', 'cast_total_facebook_likes', 'budget',
       'Canada', 'France', 'Germany', 'UK', 'USA', 'Action',
       'Adventure', 'Animation', 'Biography', 'Comedy', 'Crime', 'Documentary',
       'Drama', 'Family', 'Fantasy', 'Film-Noir', 'History', 'Horror', 'Music',
       'Musical', 'Mystery', 'Romance', 'Sci-Fi', 'Short', 'Sport', 'Thriller',
       'War', 'G', 'PG', 'PG-13', 'R', 'English', 'French', 'Spanish'],data = data)

data = data.drop(columns = ["actor_3_facebook_likes", "actor_2_facebook_likes", "actor_1_facebook_likes"])
post_data = pd.DataFrame(columns=['imdb_score'],data = data)

x_train, x_test, y_train, y_test = train_test_split(pre_data,post_data,test_size=0.3,random_state=100)

#run a regression model with statmodels (with sklearn no significance output)
import statsmodels.api as sm

# first add intercept to X (since not automatically included in ols estimation):
xc_train = sm.add_constant(x_train)
xc_test = sm.add_constant(x_test)

#train model
mod = sm.OLS(y_train,xc_train)
olsm = mod.fit()

#output table with parameter estimates (in summary2)
olsm.summary2().tables[1][['Coef.','Std.Err.','t','P>|t|']]

#predict

#Make a predictions and put this back to the dataframe called val_pred
array_pred = np.round(olsm.predict(xc_test),0) #adjust round depending on predictions

y_pred = pd.DataFrame({"y_pred": array_pred},index=x_test.index) #index must be same as original database
val_pred = pd.concat([y_test,y_pred,x_test],axis=1)
val_pred

#Evaluate model: R-square & MAE
#by comparing actual and predicted value 
from sklearn.metrics import r2_score
from sklearn.metrics import mean_absolute_error 

#input below actual and predicted value from dataframe
act_value = val_pred["imdb_score"]
pred_value = val_pred["y_pred"]

#run evaluation metrics
Model1_rsquare = r2_score(act_value, pred_value)
Model1_mae = mean_absolute_error(act_value, pred_value)
pd.DataFrame({'eval_criteria': ['r-square','MAE'],'value':[Model1_rsquare, Model1_mae]})

b_test = np.array(y_test)
pred_value = np.array(pred_value)
errors = abs(pred_value - b_test)
print('Mean Absolute Error:', round(np.mean(errors), 2), 'degrees.')

# Calculate mean absolute percentage error (MAPE)
mape = 100 * (errors / b_test)

# Calculate and display accuracy
Model1_accuracy = 100 - np.mean(mape)
print('Accuracy:', round(accuracy, 2), '%.')

# SECTION FOUR OF THE ASSIGNMENT
df = spark.createDataFrame(val_pred)

# SECTION FIVE OF THE ASSIGNMENT
df.write.json(f"s3a://dmacademy-course-assets/vlerick/allison", mode="overwrite")

# SECTION SIX OF THE ASSIGNMENT


