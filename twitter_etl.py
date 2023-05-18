import tweepy
import pandas as pd
import json
from datetime import datetime
import s3fs
from preprocess import data_cleaning
from textblob import TextBlob
from sklearn.preprocessing import MinMaxScaler


def run_twitter_etl():

    access_key = "wj75uCojSIlCSUYT2NqUXZIBI"
    access_secret = "H481c33PsbkR03tpgftSXUndBtrEWE52EYHjRVk64Z3L1F9IIM"
    consumer_key = "1012031263245467653-YDqV3vswaJtfzV1LeaiofllKJkZoIE"
    consumer_secret = "20Pf6AoMIzDW29WpbX9jJeUHT9twsE6BjVTVxXQywu4PJ"

    # Twitter authentication
    auth = tweepy.OAuthHandler(access_key, access_secret)
    auth.set_access_token(consumer_key, consumer_secret)

    # # # Creating an API object
    api = tweepy.API(auth)
    tweets = api.search_tweets(
        q='ManchesterUTD', lang='en', count=200, until='2023-05-09')

    list = []
    for tweet in tweets:
        text = tweet._json["text"]
        print(text)

        refined_tweet = {"user": tweet.user.screen_name,
                         'text': text,
                         'favorite_count': tweet.favorite_count,
                         'retweet_count': tweet.retweet_count,
                         'created_at': tweet.created_at}
        refined_tweet['text'] = data_cleaning(refined_tweet['text'])
        refined_tweet['score'] = TextBlob(
            refined_tweet['text']).sentiment_assessments[0]
        list.append(refined_tweet)

    df = pd.DataFrame(list)
    df['created_at'] = df['created_at'].dt.date
    scaler = MinMaxScaler()
    scaler.fit(df['score'].values.reshape(-1,1))
    df['score'] = scaler.transform(df['score'].values.reshape(-1,1))
    df = df.groupby(df['created_at'])['score'].mean()
    df.to_csv('s3://deltabase/bde_temp_storage/refined_tweets.csv')



