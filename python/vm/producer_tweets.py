import time
from kafka import KafkaProducer
import tweepy

API_KEY = "mwqLxJ64vVtWtoy8lO8XHnwsj"
API_KEY_SECRET = "1B5xRMLMEapdFChbMUMTWZpK17Sp9J5usv4rObZrjjqXWjIthV"
BEARER_TOKEN = "AAAAAAAAAAAAAAAAAAAAAHK4bwEAAAAA3jfvvGdvNiRZjn%2By4H4tgcGUBlg%3Dnu30ECI0qb5Io7CeYTD5DHBuy98l77WkjAquBA4oGxxrHi7ifd"
ACCESS_TOKEN = "898851040451321857-swnGgmKR2BKyV6rkivheg4jJjpa4Aws"
ACCESS_TOKEN_SECRET = "7V5DkO9GR4Sy80webTtZCgGW1tpDE1UU5fereCu20xu2X"

producer = KafkaProducer(bootstrap_servers='192.168.33.13:9092')

client = tweepy.Client(BEARER_TOKEN)


# query
# get # and word panne inside message, the tweet must be the original (not a retweet) and coming from the official account of ratp
"""
query = "#RERC panne -is:retweet -rétabli -terminé from:RERC_SNCF \
        OR #RERB panne -is:retweet -rétabli -terminé from:RERB \
        OR #RERD panne -is:retweet -rétabli -terminé from:RERD_SNCF \
        OR #RERA panne -is:retweet -rétabli -terminé from:RER_A"
"""
query = "BTC ETH -is:retweet -has:media is:verified"

tweets = client.search_recent_tweets(
    query=query, max_results=100, expansions=['author_id', 'geo.place_id'])

users = {u['id']: u for u in tweets.includes['users']}
print(tweets)

places = {p['full_name']: p for p in tweets.includes['places']}

print(places)
"""

for tweet in tweets.data:
    if users[tweet.author_id]:
        user = users[tweet.author_id]
        print(user)
    if places[tweet.geo["place_id"]]:
        place = [tweet.geo["place_id"]]
        print(place)
"""
