from tweepy.streaming import Stream
from kafka import KafkaProducer
import tweepy
import json

API_KEY = "mwqLxJ64vVtWtoy8lO8XHnwsj"
API_KEY_SECRET = "1B5xRMLMEapdFChbMUMTWZpK17Sp9J5usv4rObZrjjqXWjIthV"
BEARER_TOKEN = "AAAAAAAAAAAAAAAAAAAAAHK4bwEAAAAA3jfvvGdvNiRZjn%2By4H4tgcGUBlg%3Dnu30ECI0qb5Io7CeYTD5DHBuy98l77WkjAquBA4oGxxrHi7ifd"
ACCESS_TOKEN = "898851040451321857-swnGgmKR2BKyV6rkivheg4jJjpa4Aws"
ACCESS_TOKEN_SECRET = "7V5DkO9GR4Sy80webTtZCgGW1tpDE1UU5fereCu20xu2X"

producer = KafkaProducer(bootstrap_servers='192.168.33.13:9092')


class Listener(Stream):

    tweetsProceeded = []
    limit = 100

    def on_data(self, raw_data):
        self.process_data(raw_data)
        return True

    def process_data(self, raw_data):
        if len(self.tweetsProceeded) == self.limit:
            print("disconnecting...")
            self.disconnect()
        else:
            message = json.loads(raw_data)
            if message['user']['location'] is not None:
                print(message)
                producer.send('tweets', raw_data)
                self.tweetsProceeded.append(raw_data)

    def on_error(self, status_code):
        print(status_code)
        if status_code == 420:
            # returning false in on_data disconnects the stream
            return False


# start the stream
if __name__ == "__main__":
    auth = tweepy.OAuthHandler(API_KEY, API_KEY_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

    api = tweepy.API(auth)

    listener = Listener(API_KEY, API_KEY_SECRET,
                        ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

    keywords = ["*"]

    listener.filter(track=keywords)
