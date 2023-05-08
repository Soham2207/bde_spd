import tweepy
import pandas as pd
import json
from datetime import datetime
import s3fs


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
    tweets = api.search_tweets(q='ManchesterUTD',lang = 'en',count = 200, until= '2023-05-09')

    list = []
    for tweet in tweets:
        text = tweet._json["text"]
        print(text)

        refined_tweet = {"user": tweet.user.screen_name,
                         'text': text,
                         'favorite_count': tweet.favorite_count,
                         'retweet_count': tweet.retweet_count,
                         'created_at': tweet.created_at}

        list.append(refined_tweet)

    df = pd.DataFrame(list)
    df.to_csv('refined_tweets.csv')

run_twitter_etl()