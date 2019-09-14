#!/usr/bin/env python
# coding: utf-8

# In[3]:


# imports
from datetime import datetime
import time
import pandas as pd
import numpy as np
import matplotlib as plt
get_ipython().run_line_magic('matplotlib', 'inline')
import requests
from bs4 import BeautifulSoup

# Tweepy - Twitter API
import tweepy as tw

consumer_key = '3d36GklfJ1Vud6aG2CxZ4W5v0'
consumer_secret = 'QwPZ3L1Ef2ozwMggSrjDMPon6GgREIwSUOX1bXvmZFxK4aIvNq'
access_token = '1168946708274978816-nuiPdj6x6GziLDDujC1G3sjZOplGTJ'
access_secret = 'dbAWyRYCk0FplAfjkmqKqnw8GHdfckPB5V5yEvE2LKLtL'

auth = tw.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)

api = tw.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

import json


# In[4]:


# variables


# ### Gathering Data
# #### Read data (3 sources: twitter file, image predictions, twitter api extra data)

# In[104]:


# Read data - twitter file (csv)
df_twitter_file = pd.read_csv("twitter-archive-enhanced.csv")
pd.set_option('display.max_colwidth',-1)
# List of twitter IDs
list_tweet_id=df_twitter_file.tweet_id
# list of retweet ids
df_retweet_ids = df_twitter_file[df_twitter_file.retweeted_status_id.notnull()].tweet_id

# df_twitter_file.sample(3)
print("Number of retweets:",df_retweet_ids.shape)
df_retweet_ids.sample(3)


# In[6]:


# Read data - image predictions from udacity's url: 
# https://d17h27t6h515a5.cloudfront.net/topher/2017/August/599fd2ad_image-predictions/image-predictions.tsv

url = 'https://d17h27t6h515a5.cloudfront.net/topher/2017/August/599fd2ad_image-predictions/image-predictions.tsv'
response = requests.get(url)
df_image_pred = pd.read_csv(url,sep='\t')
# soup = BeautifulSoup(page_image_pred.content, 'html.parser')

print("Number of records in image prediction file:",df_image_pred.tweet_id.count())


# In[7]:


df_image_pred.sample(5)


# In[ ]:


# Read data - Tweet JSON
# Store in txt file and read file into a DataFrame
# https://stackabuse.com/reading-and-writing-json-to-a-file-in-python/

#Tracking how long this block takes to run
start_time = time.time()

#Extracting specific data from the API
# full_text = tweet.full_text
# favorite_count = tweet.favorite_count
# retweet_count = tweet.retweet_count

    
# recreate the file each time the code is run
with open('tweet_json.txt', 'w', encoding='utf-8') as file:
    # Loop through the list of tweet ids from the csv file, pull the twitter data via API, convert to JSON and write/append to file
    count_deleted=0
    for tid in list_tweet_id:
        try:
            tweet = api.get_status(tid,tweet_mode='extended')
            tweet_json = json.dumps(tweet._json)
            file.write(tweet_json + "\n")
        except:
            count_deleted +=1
#             print(tid, "does not exist on Twitter, moving on to next Tweet ID from csv file...")

print("\n" + str(count_deleted) + " tweet_id(s) from the CSV file could not be found on twitter")
print()
print("\n This block of code took ", (time.time()-start_time)/60)


# In[8]:


# Read the text file into a Data Frame keeping only certain columns
#Tracking how long this block takes to run
start_time = time.time()

df_list=[] #create a list to hold data as its read from the file

with open("tweet_json.txt",'r', encoding='utf-8') as file:
    line_number=0
    while line_number <= sum(1 for line in open("tweet_json.txt",'r',encoding='utf-8')):
        line_number+=1
        tweet_id=''
        retweet_count=''
        favorite_count=''
        line = file.readline()
        # Get tweet_id but check it's there first
        if line.find('"id":') != -1:
            tweet_id = line[line.find('"id":')+len('"id": ')  : line.find(",", line.find('"id":'))]
        else:
            print("'id' not found")
        # Get retweet_count but check it's there first
        if line.find('"retweet_count":') != -1:
            retweet_count = line[line.find('"retweet_count":')+len('"retweet_count": ') : line.find(",", line.find('"retweet_count":'))]
        else:
            print("'retweet_count not found")
        # Get favorite_count but check it's there first
        if line.find('"favorite_count":') != -1:
            favorite_count = line[line.find('"favorite_count":')+len('"favorite_count": ') : line.find(",", line.find('"favorite_count":'))]
        else:
            print("'favorite_count not found")

        # Append each entry from the JSON file to the list
        if tweet_id != '':
            df_list.append({'tweet_id':tweet_id,
                            'retweet_count':retweet_count,
                            'favorite_count':favorite_count})
        else:
            print("not updating DataFrame due to tweet_id not being found")

# Create data from from JSON file with specified columns
df_json = pd.DataFrame(df_list, columns = ['tweet_id','retweet_count','favorite_count'])


print("\n number of entries in data frame, including heading:", len(df_json))
print("\n number of entries in the file", sum(1 for line in open("tweet_json.txt",'r',encoding='utf-8')))
print()
print("\n This block of code took ", (time.time()-start_time)/60)


# ### Assess Data

# ##### Summary of findings based on code below
# 
# ##### Twitter flat file
# - (1) After eyeballing the data, Col: name has dirty data e.g. "a" , "an", "the", etc. Some names are set to "None"
#     - col: name - set None to null?
# - (A) combine the dog stages into one column with a label (doggo, floofer etc. into one column but must ensure a row does not have two dog stages
#      - (8) very few dog stages have been identified, might be problem with the file - can write code to check if we can get more dog stages from col: text
#      - Dog stages - looks like some dogs are in two stages - worth exploring to see if this is an error
#        On inspecting the source data i.e. the tweets, it doesn't look to be - people just quote two dog stages (i.e. they share a picture of their two dogs) and it get's recorded. 
#        
# - after running the below code, some expanded urls:
#     - (2) not every entry has an url - when a photo is not uploaded - 59 (3) records identified without a url - this could be a reply or retweet without a picture
#       - should not have a dog type or an entry in the image_pred file
#     - (B) have more than one urls - this duplication is caused when a person uploads more than one photo in their tweet (Twitter allows up to 4 photos) or 
#     - (3) they include a url to another site in their tweet. 
#         - can clean this up by:
#             - removing duplicates 
#             - (3) removing non twitter urls or put in own column
#     - (2) - some urls have videos only (you can only have 1 video per tweet)
# 
# - (4) col: Source - contains a full html tag
# 
# - (C) col: retweet_status_id - Contains ReTweets ids - indicates a retweet (RT - columns retweeted_status_id.notnull())
# 
# - (5) col: rating_Denominator - some denominators are not 10
# - (6) col: rating_numerator - some numerators are extreme
#     - some information for both columns are captured incorrectly. For example, 786709082849828864 has a 9.75/10 rating. Another example is 810984652412424192 where the text includes "smiles 24/7" but was captured as a rating - no other rating info present. 
# 
# - (D) Col retweet* cols should be in their own data set
# - (D) Col dog type and rating info in their own data set
# 
# - (E) col timestamp and retweet*timestamp is set to object
# 
# ##### Image prediction file
# - (7) col: jpg_url contains duplicate images - most likely belonging to retweets. Identify retweets from the main Twitter file
# - (9) remove records where a dog has not been identified
# 
# ##### json file
# - (F) col: retweet_count and favorite_count are string objects instead of integer

# In[9]:


# Assess the 3 files

print("downloaded twitter file")
df_twitter_file.info(null_counts=True)
print()
print("image prediction file")
df_image_pred.info(null_counts=True)
print()
print("Twitter API data extraction file")
df_json.info(null_counts=True)


# In[10]:


print("Number of missing expanded_URLS from the twitter file is:", df_twitter_file[df_twitter_file.expanded_urls.isnull()].tweet_id.count())
print("Number of tweets in the Twitter downloaded file that contains RTs:",df_twitter_file[df_twitter_file.retweeted_status_id.notnull()].tweet_id.count())
# df_twitter_file.sample(5)
# print()
# print(df_image_pred.sample(3))
# print()
# print(df_json.sample(3))
# df_twitter_file[df_twitter_file.tweet_id == 783695101801398276]
# df_json[df_json.tweet_id== 886267009285017600]
# df_twitter_file[df_twitter_file.expanded_urls.isnull()]


# In[11]:


df_twitter_file.describe()


# In[12]:


df_image_pred.describe()
df_image_pred.sample(3)
df_image_pred[df_image_pred.tweet_id == 862096992088072192]


# In[13]:


df_json.describe()


# In[14]:


df_twitter_file[df_twitter_file.tweet_id.duplicated()]
#no duplicated ids


# In[15]:


df_image_pred[df_image_pred.tweet_id.duplicated()]
#no duplicated ids
# df_image_pred[df_image_pred.jpg_url == 'https://pbs.twimg.com/media/CWza7kpWcAAdYLc.jpg']
# df_twitter_file[df_twitter_file.tweet_id == 679158373988876288]
# df_twitter_file[df_twitter_file.tweet_id == 754874841593970688]


# In[16]:


df_json[df_json.tweet_id.duplicated()]
#no duplicate ids


# In[17]:


df_twitter_file[df_twitter_file.in_reply_to_status_id.notnull()].sample(3)
#tweets that seem to have a reply have a bad rating or multiple, have no expanded_urls ALTHOUGH
# SOME are valid with photos and without photos


# In[18]:


# df_twitter_file: Identify bad names

# df_twitter_file.name.value_counts()
df_twitter_file[df_twitter_file['name'].str.contains('^[a-z]+')].name.unique()
# Bad names include: 'such', 'a', 'quite', 'not', 'one', 'incredibly', 'mad', 'an',
#       'very', 'just', 'my', 'his', 'actually', 'getting', 'this',
#       'unacceptable', 'all', 'old', 'infuriating', 'the', 'by',
#       'officially', 'life', 'light', 'space'
# change "None to NaN"


# In[19]:


# df_twitter_file.rating_denominator.value_counts()
# print(df_twitter_file[df_twitter_file.rating_denominator !=10].sample(3))/

print("Rating_Denominator values excluding 10")
df_twitter_file[df_twitter_file['rating_denominator'] != 10].groupby('rating_denominator')['rating_denominator'].count()


# - By my understading: The denominators should all be ten
#     - Solution: rebase all denominator values to 10? 

# In[20]:


df_twitter_file.rating_numerator.value_counts()


# In[21]:


df_twitter_file[df_twitter_file.rating_numerator >= 20].sample(3)


# - a few extreme values but majority of people keep a number below 20

# In[22]:


print("Checking if tweet_id is unique. Expecting 2,356 values, counting: ",df_twitter_file.tweet_id.value_counts().size)


# In[23]:


print("\n doggo")
print(df_twitter_file.doggo.value_counts())
print("\n floofer")
print(df_twitter_file.floofer.value_counts())
print("\n pupper")
print(df_twitter_file.pupper.value_counts())
print("\n puppo")
print(df_twitter_file.puppo.value_counts())


# In[24]:


assess_dog_stages = df_twitter_file[['doggo','floofer','pupper','puppo']].drop_duplicates()

assess_dog_stages

# Example of a tweet with a dog in two stages or two or more dogs in the picture at different stages
# Getting URL to understand the data better and to determine if this is untidy data
# Conclusion: not untidy data however, some rows have duplicate URLs
pd.set_option('display.max_colwidth',-1)
df_twitter_file.expanded_urls[df_twitter_file['doggo']=='doggo'][df_twitter_file['pupper']=='pupper']


# In[25]:


# Checking col: expanded_urls 
# trying to understand why some rows have multiple urls that are the same
    # reason1: when a tweet has more than one photo, the url is captured as many times as there are photos
    # solution1: remove all duplicate urls, or change the urls incrementing the photo number by 1 for each
    # reason2: people include urls in their tweets
    # solution2: remove non twitter urls? 
    
# Copy dataframe - adding new columns
df_urls=df_twitter_file.copy()

df_urls['comma_count']=df_urls.expanded_urls.str.count(',')
df_urls['non_twit_acc']=df_urls.expanded_urls.str.startswith('https://twitter.com')

print("number of rows with non twitter urls: ", df_urls[df_urls['non_twit_acc']==False].shape)
print("number of rows with more than 1 url: ", df_urls[df_urls['comma_count'] >=1].shape)
print("number of rows with more than 1 twitter urls: ",df_urls[df_urls['comma_count'] >=1][df_urls['non_twit_acc'] == True].shape)


# ### Clean data

# ##### Twitter file
# - col: identify ReTweets and replies and remove from file
# - col: name - remove non-names (they start with lowercase letters)
# - col: expanded_urls: split out non-twitter urls into its own column
# - col: expanded_urls: a tweet with more one url indicates a tweet with more than one photo, identify the number of photos and create a column to capture the number of photos included in each tweet. In the expanded_urls column, only include a link to the tweet itself. 
# - col: expanded_urls: for missing urls, create url based on tweet_id and basic structure of a tweet e.g. "https://twitter.com/dog_rates/status/"
# - Update dog stage
# - Correct faulty raitings
# - Change col timestamp and datetime variable
# - Remove HTML tags from the source column
# - drop retweet and reply columns
# 
# 
# 

# In[152]:


#Copy the DataFrames - keeping the orginal data for comparison to the clean data later
dfc_twitter_file = df_twitter_file.copy()
print("\ndfc_twitter_file - Total number of records before cleaning:",dfc_twitter_file.tweet_id.value_counts().size)
dfc_image_pred = df_image_pred.copy()
print("\ndfc_image_pred - Total number of records before cleaning:",dfc_image_pred.tweet_id.value_counts().size)
dfc_json = df_json.copy()
print("\ndfc_json - Total number of records before cleaning:",dfc_json.tweet_id.value_counts().size)


# In[153]:


# dfc_twitter_file: Remove tweets that are replies
dfc_twitter_file = dfc_twitter_file[dfc_twitter_file.in_reply_to_status_id.isnull()]
# dfc_twitter_file: Remove tweets that are retweets
dfc_twitter_file = dfc_twitter_file[dfc_twitter_file.retweeted_status_id.isnull()]

# dfc_twitter_file: test removal of replies and retweets
print("dfc_twitter_file: Are there any tweets that are replies?")
print(dfc_twitter_file[dfc_twitter_file.in_reply_to_status_id.notnull()])
print("\ndfc_twitter_file: Are there any tweets that are retweets?")
print(dfc_twitter_file[dfc_twitter_file.retweeted_status_id.notnull()])

print("\ndfc_twitter_file: Total number of records after removing replies and retweets: ", dfc_twitter_file.tweet_id.value_counts().size)


# In[154]:


# dfc_twitter_file: drop replies and retweet columns
# in_reply_to_status_id
# in_reply_to_user_id
# retweeted_status_id 
# retweeted_status_user_id
# retweeted_status_timestamp

print("dfc_twitter_file: The number of columns in the file before they are dropped:",dfc_twitter_file.shape[1])

# Drop columns based on position in file
dfc_twitter_file.drop(dfc_twitter_file.columns[[1,2,6,7,8]], axis = 1, inplace=True)
             
print("dfc_twitter_file: The number of columns in the file after they are dropped:",dfc_twitter_file.shape[1])
print("\ndfc_twitter_file: Total number of records after adding missing urls: ", dfc_twitter_file.tweet_id.value_counts().size)


# In[155]:


# dfc_twitter_file: Visual check for dropped columns
dfc_twitter_file.sample(3)


# In[156]:


# dfc_twitter_file: Remove bad names
print("A list of names starting with lowercase: ",dfc_twitter_file[dfc_twitter_file['name'].str.contains('^[a-z]+')].name.unique())


# In[157]:


# dfc_twitter_file: Identify bad names and replace with None
print("dfc_twitter_file: The current total number of names set to 'None' (before fix):",dfc_twitter_file[dfc_twitter_file['name']=='None'].name.count())

# get a unique list of bad names - bad names start with lowercase
bad_names = dfc_twitter_file[dfc_twitter_file['name'].str.contains('^[a-z]+')].name.unique()

# check the number of bad names
print("\ndfc_twitter_file: A list and count of bad names being replaced by 'None'\n")
for name in bad_names:
    print(name,"(",dfc_twitter_file.loc[dfc_twitter_file['name']==name].name.count(),")")
    dfc_twitter_file.name = dfc_twitter_file.name.replace(name,"None")


# In[158]:


print("dfc_twitter_file: Total number of names set to 'None':",dfc_twitter_file[dfc_twitter_file['name']=='None'].name.count())

print("\ndfc_twitter_file: Total number of records after adding missing urls: ", dfc_twitter_file.tweet_id.value_counts().size)


# In[159]:


# DO NOT DELETE OR CHANGE EXPANDED_URLS 
# KEEP THE FIRST URL IN THE LIST
# PUT NON-TWITTER URLS IN ANOTHER COLUMN
# RECREATE ALL URLS TO POINT TO TWITTER USING THE TWEET ID IN A NEW COLUMN CALLED: TWITTER URL

# dfc_twitter_file.expanded_urls.str.slice(start=0, stop=len(dfc_twitter_file.expanded_urls.str.split(",",n=1)),step=1)
# dfc_twitter_file.expanded_urls.str.split(",",n=1).str[0].str.startswith("https://twitter.com/")

# Create a tempory column to hold urls
dfc_twitter_file['temp_url']=dfc_twitter_file.expanded_urls.str.split(",",n=1).str[0]
dfc_twitter_file.temp_url

# create a new column to hold non twitter urls
dfc_twitter_file['non_twitter_url']=dfc_twitter_file[~dfc_twitter_file.temp_url.str.startswith("https://twitter.com/", na=False)].temp_url

# create a column to hold twitter urls
dfc_twitter_file['twitter_url']=dfc_twitter_file[dfc_twitter_file.temp_url.str.startswith("https://twitter.com/", na=False)].temp_url
# remove video/1 or photo/n from url leaving url in format https://twitter.com/dog_rates/status/tweet_id
dfc_twitter_file.twitter_url = dfc_twitter_file.twitter_url.str.slice(start=0, stop=len('https://twitter.com/dog_rates/status/')+18, step=1)

dfc_twitter_file.drop('temp_url',axis = 1, inplace=True)

# dfc_twitter_file.info()

dfc_twitter_file.sample(3)


# In[160]:


# dfc_twitter_file: Fill in missing URLs using the tweet_id
print("dfc_twitter_file: The number of missing urls before fix: ", dfc_twitter_file.tweet_id.value_counts().size - dfc_twitter_file[dfc_twitter_file.twitter_url.notnull()].twitter_url.count())
dfc_twitter_file.tweet_id.value_counts().size

dfc_twitter_file["twitter_url"].fillna("https://twitter.com/dog_rates/status/"+dfc_twitter_file.tweet_id.apply(str), inplace = True)
print(dfc_twitter_file[dfc_twitter_file.twitter_url.isnull()])
print("dfc_twitter_file: The number of missing urls after fix: ", dfc_twitter_file.tweet_id.value_counts().size - dfc_twitter_file[dfc_twitter_file.twitter_url.notnull()].twitter_url.count())
print("\ndfc_twitter_file: Total number of records after adding missing urls: ", dfc_twitter_file.tweet_id.value_counts().size)


# In[161]:


# Count the number of Twitter URL's and store the number in a new column called photo_per_tweet

print("The number of tweets with n photos:\n",dfc_twitter_file.expanded_urls.str.count("https://twitter.com/").value_counts())
dfc_twitter_file['photo_per_tweet'] = dfc_twitter_file.expanded_urls.str.count("https://twitter.com/")
print("\nThe number of tweets with n photos in column photo_per_tweet:\n",dfc_twitter_file.photo_per_tweet.value_counts())


# dfc_twitter_file[dfc_twitter_file.tweet_id == 706153300320784384]


# In[162]:


# dfc_twitter_file: create a column for dog status (category type)
# search the text for a list of dog stages
# dfc_twitter_file.sample(15)


df = dfc_twitter_file.copy()

search = ['doggo', 'floofer', 'pupper', 'puppo']
# search the text for the 4 dog stages and add them to a list. Convert this list to a string and store in column dog_stage
dfc_twitter_file['dog_stage'] = dfc_twitter_file['text'].str.findall('|'.join(search)).apply(' '.join)
dfc_twitter_file['dog_stage'] = dfc_twitter_file['dog_stage'].apply(lambda x: ' '.join(sorted(x.split())))
  
dfc_twitter_file.dog_stage = dfc_twitter_file.dog_stage.replace('pupper pupper pupper', 'pupper')
dfc_twitter_file.dog_stage = dfc_twitter_file.dog_stage.replace('doggo doggo pupper', 'doggo pupper')
dfc_twitter_file.dog_stage = dfc_twitter_file.dog_stage.replace('pupper pupper', 'pupper')

# df[df.dog_stage=='pupper, pupper, pupper']

dfc_twitter_file.copy().dog_stage.value_counts()
# df.sample(10)
# df.dog_stage.dtype
# df.info()


# In[163]:


# dfc_twitter_file: fix the rating_denominator and numerator
# Focus on the odd denominators as a first
dfc_twitter_file.rating_denominator.value_counts()
# dfc_twitter_file[dfc_twitter_file.rating_denominator != 10]


# In[164]:


dfc_twitter_file.rating_numerator.value_counts()


# In[165]:


# dfc_twitter_file: fix the rating_denominator and numerator

# manual change ratings by reviewing the tweets
dfc_twitter_file.loc[df.tweet_id == 740373189193256964, ['rating_numerator', 'rating_denominator']] = 14, 10
dfc_twitter_file.loc[df.tweet_id == 722974582966214656, ['rating_numerator', 'rating_denominator']] = 13, 10
dfc_twitter_file.loc[df.tweet_id == 722974582966214656, ['rating_numerator', 'rating_denominator']] = 13, 10
dfc_twitter_file.loc[df.tweet_id == 716439118184652801, ['rating_numerator', 'rating_denominator']] = 11, 10
dfc_twitter_file.loc[df.tweet_id == 682962037429899265, ['rating_numerator', 'rating_denominator']] = 10, 10
dfc_twitter_file.loc[df.tweet_id == 666287406224695296, ['rating_numerator', 'rating_denominator']] = 9, 10
# set this one to 10/10 as there was no rating
dfc_twitter_file.loc[df.tweet_id == 810984652412424192, ['rating_numerator', 'rating_denominator']] = 10, 10

# For the remaining records with a denominator not set to 10, programmatically fix - base denominator to 10 and rebase numerator
dfc_twitter_file['rating_numerator'] = dfc_twitter_file.loc[dfc_twitter_file['rating_denominator'] > 10, 'rating_numerator'] = dfc_twitter_file.rating_numerator / (dfc_twitter_file.rating_denominator / 10)
# changes denominator (do this after changing numerator)
dfc_twitter_file['rating_denominator'] = dfc_twitter_file.loc[dfc_twitter_file['rating_denominator'] > 10, 'rating_denominator'] = dfc_twitter_file.rating_denominator / (dfc_twitter_file.rating_denominator / 10)


dfc_twitter_file.rating_denominator.value_counts()


# In[166]:


dfc_twitter_file.rating_numerator.value_counts()


# In[167]:


# dfc_twitter_file[dfc_twitter_file.rating_denominator != 10]
dfc_twitter_file[dfc_twitter_file.rating_denominator != 10]


# In[ ]:





# In[168]:


# dfc_twitter_file: change numeric objects (timestamp) to integers/date in the various tables 
print("Check to see the column data types before they are changed accordingly\n")
print(dfc_twitter_file.dtypes)


# In[169]:


# Change timestamp to datetime
dfc_twitter_file['timestamp'] = pd.to_datetime(dfc_twitter_file['timestamp'])
# change photo_per_tweet from float to int8 (save memory) and also fill any missing values as 0
dfc_twitter_file['photo_per_tweet'] = dfc_twitter_file['photo_per_tweet'].fillna(0).astype(np.int8)
# change rating columns 
dfc_twitter_file.rating_numerator = dfc_twitter_file.rating_numerator.astype(np.int64)
dfc_twitter_file.rating_denominator = dfc_twitter_file.rating_denominator.astype(np.int64)

# dfc_twitter_file.sample(5)

# A print out of the dataframe info to see what has changed 
print("Check to see if the column data types have changed accordingly")
print(dfc_twitter_file.dtypes)


# In[170]:


dfc_twitter_file.rating_numerator.value_counts()
dfc_twitter_file[dfc_twitter_file.rating_numerator < 0]


# In[171]:


dfc_twitter_file.rating_denominator.value_counts()


# In[172]:


# dfc_twitter_file
# Clean the source column removing HTML tags

print("List of unique values in source column before removing html tags:\n",dfc_twitter_file.source.unique())


# In[173]:


# Use of Beautiful soup and lambda to remove html tags from source column
dfc_twitter_file['source'] = dfc_twitter_file.source.apply(lambda x: BeautifulSoup(x,'lxml').get_text())


print("List of unique values in source column after removing html tags:\n",dfc_twitter_file.source.unique())
print()
dfc_twitter_file.sample(3)


# In[174]:


#dfc_twitter_file
# drop records with no photos
# drop the doggo, floofer, pupper, puppo, expanded_urls columns
# reorder the columns

print("The shape of the data before records with no photos are removed:",dfc_twitter_file.shape)


# In[175]:


# Count after removing records without photos

dfc_twitter_file = dfc_twitter_file[dfc_twitter_file.photo_per_tweet > 0]
print("The shape of the data after records with no photos are removed:",dfc_twitter_file.shape)


# In[176]:


# drop the doggo, floofer, pupper, puppo, expanded_urls columns

print("The shape of the data before columns are removed:",dfc_twitter_file.shape)


# In[177]:


# drop columns not needed

dfc_twitter_file.drop(columns=['doggo','floofer','pupper','puppo','expanded_urls'], inplace=True)

print("The shape of the data after columns are removed:",dfc_twitter_file.shape)

dfc_twitter_file.info()


# In[178]:


#dfc_twitter_file: reorder the columns

print("Column order before\n")
dfc_twitter_file.info()


# In[179]:


# get a list of tweet_ids
s_tweet_ids = dfc_twitter_file.tweet_id
s_tweet_ids.shape


# ### Clean Data

# 
# ##### Image Pred 
# - Remove retweets (use (retweet) tweet_id list from twitter file)
# - Restructure file (p1, p2, etc.)
# - Exclude records where no picture has been identified as a dog

# In[180]:


# image_pred: 

# dfc_image_pred.info()
# dfc_image_pred.describe()
print("p1 median:",dfc_image_pred.p1_conf.median())
print("p2 max:",dfc_image_pred.p2_conf.max())
print("p3 max:",dfc_image_pred.p3_conf.max())

dfc_image_pred.sample(5)
dfc_image_pred.shape


# In[181]:


dfc_image_pred[dfc_image_pred.tweet_id == 666020888022790149].head(5)


# In[182]:


# df_image_pred - restructure the file
# p1 to p3 into one column for type and rating and boolean 
df_backup = dfc_image_pred.copy()


df1= pd.melt(dfc_image_pred,id_vars=['tweet_id','jpg_url','img_num']
        ,var_name=['ranking']
        ,value_vars=['p1','p2','p3']
        ,value_name='dog_type'
       ).sort_values('tweet_id')
        
df2= pd.melt(dfc_image_pred,id_vars=['tweet_id','jpg_url','img_num']
       ,var_name=['ranking']
       ,value_vars=['p1_conf','p2_conf','p3_conf']        
       ,value_name='confidence_of_prediction'
       ).sort_values('tweet_id')
# clean ranking

df2.ranking = df2.ranking.str[:2]
#This one is a bit slow
# df2.ranking = df2.ranking.apply(lambda x: df2.ranking.str.split("_",n=1).str[0]) 

df3= pd.melt(dfc_image_pred,id_vars=['tweet_id','jpg_url','img_num'],
       value_vars=['p1_dog','p2_dog','p3_dog']
        ,var_name=['ranking']
       ,value_name='is_dog'
       ).sort_values('tweet_id')
# clean ranking
df3.ranking = df3.ranking.str[:2]

# Merge data frames 
df_new = pd.merge(df1,df2, how='left'
                  , on=('tweet_id','jpg_url','img_num','ranking')
                  
                 )
dfc_image_pred= pd.merge(df_new,df3, how='left'
                 , on=('tweet_id','jpg_url','img_num','ranking')
                ).sort_values('tweet_id')

# df2.head(10)
# df_image.head(10)
# df_image.sample(10)
# df_image.head(20)
# df2.head()
# df_image.shape
# df.dtypes


# In[183]:


# df1[df1.tweet_id==666020888022790149].head(10)
dfc_image_pred.tail(10)
# df_image.shape


# In[184]:


# dfc_image_pred: remove all records that are not dogs i.e. is_dog = False
print("Number of records by variable 'is_dog':\n",dfc_image_pred.is_dog.value_counts())


# In[185]:


# dfc_image_pred: remove all records that are not dogs i.e. is_dog = False
dfc_image_pred = dfc_image_pred[dfc_image_pred.is_dog != False]
print("Number of records by variable 'is_dog':\n",dfc_image_pred.is_dog.value_counts())


# In[186]:


# dfc_twitter_file: Keep only the list of tweets from the twitter file
# Thereby, removing all non necessary rows of data

print("Number of rows currently in the data before we remove the non essential rows:",dfc_image_pred.shape)


# In[187]:


# dfc_twitter_file: Keep only the list of tweets from the twitter file
dfc_image_pred= dfc_image_pred[dfc_image_pred.tweet_id.isin(s_tweet_ids)]

print("Number of rows after we keep only the tweet ids that match what we have in the twitter file:",dfc_image_pred.shape)


# In[188]:


# check the types of the dfc_image_pred dataframe
dfc_image_pred.dtypes


# ### Clean Data

# 
# ##### twitter_json
# - Remove retweets (use (retweet) tweet_id list from twitter file) - maybe
# - Change the *_count columns to integers from objects & the tweet_id col to integer
# - Move count columns to the twitter file dataframe

# In[189]:


print("Shape of the json data:\n",dfc_json.shape)

dfc_json.dtypes


# In[190]:


# Change data types of dfc_json file

dfc_json.tweet_id = dfc_json.tweet_id.astype(np.int64)
dfc_json.retweet_count = dfc_json.retweet_count.astype(np.int64)
dfc_json.favorite_count = dfc_json.favorite_count.astype(np.int64)


print("Data types after the change:\n")
dfc_json.dtypes


# In[191]:


# dfc_json: Keep only the list of tweets from the twitter file
print("Number of rows before we keep only the tweet ids that match what we have in the twitter file:",dfc_json.shape)

dfc_json= dfc_json[dfc_json.tweet_id.isin(s_tweet_ids)]

print("Number of rows after we keep only the tweet ids that match what we have in the twitter file:",dfc_json.shape)


# ### Clean data

# ##### twitter_file
# - add columns from json file to main twitter_file (use fillna to fill in missing values)
# 

# In[208]:


# Join data from json df to twitter_file df

print("Number of columns on the twitter_file dataframe before adding two more:\n",dfc_twitter_file.shape)
df= pd.merge(dfc_twitter_file,dfc_json, how='left'
                 , on='tweet_id'
                ).sort_values('tweet_id')

print("Number of missing retweet counts:",df.retweet_count.isnull().sum())
print("Number of missing favourite counts:",df.favorite_count.isnull().sum())


# In[211]:


# Fill in missing values using mean
df.retweet_count = df.retweet_count.fillna(df.retweet_count.mean())
df.favorite_count = df.favorite_count.fillna(df.favorite_count.mean())

print("Number of missing retweet counts:",df.retweet_count.isnull().sum())
print("Number of missing favourite counts:",df.favorite_count.isnull().sum())


# In[212]:


df.dtypes


# In[213]:


# change data type of the two columns added


df.retweet_count = df.retweet_count.astype(np.int64).fillna(df.mean())
df.favorite_count = df.favorite_count.astype(np.int64).fillna(df.mean())

df.dtypes


# In[195]:





# In[ ]:




