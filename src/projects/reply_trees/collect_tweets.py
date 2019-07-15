import os

from src.etl.twitter_fetcher import TwitterFetcher
from utils import get_project_root


if __name__ == '__main__':
    with open(os.path.join(get_project_root(), 'src/projects/reply_trees/news_sites'), "r") as f:
        news_sites = f.readlines()

    tf = TwitterFetcher()
    data = tf.get_data(news_sites=news_sites, sleep_time=60)

    data.to_csv(os.path.join(get_project_root(), 'datasets/raw/tweets.txt'), index=False, header=False)
