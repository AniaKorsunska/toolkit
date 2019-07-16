import pandas as pd
import twitter
import os
import time
import urllib.parse

from src.etl.fetcher import Fetcher
from utils import get_configuration_from_file, get_project_root


class TwitterFetcher(Fetcher):
    """
    Twitter fetcher. Scraps data from twitter and returns a pd.Dataframe.
    """

    def __init__(self, **kwargs):
        Fetcher.__init__(self)

        self.config = get_configuration_from_file(os.path.join(get_project_root(), 'configuration',
                                                               'twitter-configuration.json'))
        self.twitter_api = self._set_environment(self.config)

    def _fetch(self, **kwargs):
        """
        Implements the main logic of the class.

        :param kwargs: - url: str
                       - sleep_time: int
        :return: pd.Dataframe
        """
        return pd.DataFrame(self._get_tweets(kwargs['news_sites'], kwargs['sleep_time']))

    def _get_tweets(self, news_sites, sleep_time):
        """
        It fetches tweets using Twitter API

        :param news_sites: str, twitter user accounts screen names
        :param sleep_time: int, seconds to delay execution
        :return: list of Status objects
        """
        tweets = []

        for news_site in news_sites:
            q = urllib.parse.urlencode({"q": "to:%s" % news_site})
            try:
                tweets = tweets + self.twitter_api.GetSearch(raw_query=q)
            except twitter.error.TwitterError as ex:
                self.logger.error("caught twitter api error: %s", ex)
                time.sleep(sleep_time)
                continue

        return tweets

    @staticmethod
    def _set_environment(configuration):
        """ Utility method to set twitter related environment variables """
        return twitter.Api(
            consumer_key=configuration.CONSUMER_KEY,
            consumer_secret=configuration.CONSUMER_SECRET,
            access_token_key=configuration.ACCESS_TOKEN,
            access_token_secret=configuration.ACCESS_TOKEN_SECRET,
            sleep_on_rate_limit=True
        )
