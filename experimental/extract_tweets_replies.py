"""
Twitter's API doesn't allow you to get replies to a particular tweet.
Thus, we used Twitter's Search API to search for tweets that are
directed at a particular user, and then searched through the results to see if
any are replies to a given tweet.

We were also interested in the replies to any replies as well, so the process is recursive.
The big caveat here is that the search API only returns results for the last 7 days.

This scripts takes as input a line oriented JSON file of tweets and look for replies
using the above heuristic. Any replies that are discovered will be written as
line oriented JSON to stdout.

"""

import json
import time
import twitter
import urllib.parse
import os

from os import environ as e

from utils import get_project_root

t = twitter.Api(
    consumer_key=e["CONSUMER_KEY"],
    consumer_secret=e["CONSUMER_SECRET"],
    access_token_key=e["ACCESS_TOKEN"],
    access_token_secret=e["ACCESS_TOKEN_SECRET"],
    sleep_on_rate_limit=True
)


def tweet_url(tweet):
    """
    Given a tweet object, it creates its url.

    :param tweet: tweet obj
    :return: str, tweet's url
    """
    return "https://twitter.com/%s/status/%s" % (tweet.user.screen_name, tweet.id)


def get_tweets(filename):
    """
    Read tweets from a line-oriented JSON file.

    :param filename: str
    :return: generator, with each of the tweets in json format
    """
    for line in open(filename):
        yield twitter.Status.NewFromJsonDict(json.loads(line))


def get_replies(tweet):
    """
    Get all replies for a given tweet in a recursive way.

    :param tweet: tweet object
    :return: generator, with the replies as tweet objects
    """

    user = tweet.user.screen_name
    tweet_id = tweet.id
    max_id = None

    print("looking for replies to: %s" % tweet_url(tweet))
    while True:
        q = urllib.parse.urlencode({"q": "to:%s" % user})
        try:
            replies = t.GetSearch(raw_query=q, since_id=tweet_id, max_id=max_id, count=100)
        except twitter.error.TwitterError as ex:
            print("caught twitter api error: %s", ex)
            time.sleep(60)
            continue

        for reply in replies:

            # print("examining: %s" % tweet_url(reply))
            if reply.in_reply_to_status_id == tweet_id:
                print("found reply: %s" % tweet_url(reply))
                yield reply

                # recursive way to also get the replies to this reply
                for reply_to_reply in get_replies(reply):
                    yield reply_to_reply

            max_id = reply.id

        if len(replies) != 100:
            break


if __name__ == "__main__":
    tweets_file = os.path.join(get_project_root(), "datasets/raw/tweets.jsonl")
    for tweet_json in get_tweets(tweets_file):
        for reply_ in get_replies(tweet_json):
            rep = reply_.AsJsonString()
            to_store = [rep.created_at, rep.id, rep.full_text, len(rep.entities.hashtags), len(rep.entities.urls),
                        len(rep.entities.user_mentions), rep.source, rep.user.id, rep.user.screen_name,
                        rep.user.location, rep.user.followers_count, rep.user.friends_count, rep.user.created_at,
                        rep.user.favourites_count, rep.user.verified, rep.user.statuses_count, rep.retweet_count,
                        rep.favorite_count, rep.favorited, rep.retweeted]
            print(to_store)
