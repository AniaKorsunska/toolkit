import time
import json
import twitter
import urllib.parse
import pandas as pd

from src.etl.fetcher import Fetcher


class TwitterRepliesFetcher(Fetcher):
    """
    Twitter fetcher for replies. Scraps data from twitter and returns a pd.Dataframe.
    """

    def __init__(self, **kwargs):
        Fetcher.__init__(self)

    def _fetch(self, **kwargs):
        """
        Implements the main logic of the class.

        :param kwargs: - url: str
        :return: pd.Dataframe
        """
        replies = []

        for tweet in kwargs['tweets']:
            replies.append(self._format_data(tweet))
            for reply in self._get_reply(tweet):
                to_store = self._format_data(reply.AsJsonString())
                replies.append(to_store)

        return pd.DataFrame(replies)

    def _get_reply(self, tweet):
        """
        Get all replies for a given tweet in a recursive way.

        :param tweet: tweet object
        :return: generator, with the replies as tweet objects
        """
        user = tweet.user.screen_name
        tweet_id = tweet.id
        max_id = None

        self.logger.debug("looking for replies to: %s" % self._tweet_url(tweet))
        while True:
            q = urllib.parse.urlencode({"q": "to:%s" % user})
            try:
                replies = tweet.GetSearch(raw_query=q, since_id=tweet_id, max_id=max_id, count=100)
            except twitter.error.TwitterError as ex:
                self.logger.error("caught twitter api error: %s", ex)
                time.sleep(60)
                continue

            for reply in replies:

                if reply.in_reply_to_status_id == tweet_id:
                    self.logger.debug("found reply: %s" % self._tweet_url(reply))
                    yield reply

                    # recursive way to also get the replies to this reply
                    for reply_to_reply in self._get_reply(reply):
                        yield reply_to_reply

                max_id = reply.id

            if len(replies) != 100:
                break

    @staticmethod
    def _tweet_url(tweet):
        """
        Given a tweet object, it creates its url.

        :param tweet: tweet obj
        :return: str, tweet's url
        """
        return "https://twitter.com/%s/status/%s" % (tweet.user.screen_name, tweet.id)

    @staticmethod
    def _get_tweets(filename):
        """
        Reads tweets from a line-oriented JSON file.

        :param filename: str
        :return: generator, with each of the tweets in json format
        """
        for line in open(filename):
            yield twitter.Status.NewFromJsonDict(json.loads(line))

    @staticmethod
    def _format_data(tweet):
        """
        Formats json-like data to be stored in a comprehensive way.

        :param tweet:
        :return:
        """
        return [tweet.created_at, tweet.id, tweet.full_text, len(tweet.entities.hashtags), len(tweet.entities.urls),
                len(tweet.entities.user_mentions), tweet.source, tweet.user.id, tweet.user.screen_name,
                tweet.user.location, tweet.user.followers_count, tweet.user.friends_count, tweet.user.created_at,
                tweet.user.favourites_count, tweet.user.verified, tweet.user.statuses_count, tweet.retweet_count,
                tweet.favorite_count, tweet.favorited, tweet.retweeted]
