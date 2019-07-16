"""
This script fetches tweets from twitter news profiles.
Given a list with news portals' twitter profiles, it gets recent tweets and stores them in json-line format.
Then it fetches for each tweet its reply tree and stores all (tweets and replies).

"""

import os

from src.etl.twitter_fetcher import TwitterFetcher
from src.etl.twitter_fetcher_replies import TwitterRepliesFetcher
from utils import get_project_root


if __name__ == '__main__':
    # get the news sources twitter profile names
    with open(os.path.join(get_project_root(), 'src/projects/reply_trees/news_sites'), "r") as f:
        news_sites = f.readlines()

    # get raw data from twitter
    tf = TwitterFetcher()
    tweets = tf.get_data(news_sites=news_sites, sleep_time=60)

    # save to json-line format
    with open(os.path.join(get_project_root(), 'datasets/raw/tweets_new.jsonl'), 'a') as the_file:
        for d in tweets.values:
            the_file.write(d[0].AsJsonString())
            the_file.write('\n')

    # pass tweets to replies fetcher to get the tweets with their replies tree
    tfr = TwitterRepliesFetcher()
    replies = tfr.get_data(tweets=tweets[0].values)

    # save to csv
    replies.to_csv(os.path.join(get_project_root(), 'datasets/processed/replies.csv'), header=False, index=False)
