import string
from pyspark.sql import SparkSession, Row
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql.types import FloatType, StringType, IntegerType
from pyspark.sql.functions import to_date
import json
import re

emoji_pattern = re.compile("["
        u"\U0001F600-\U0001F64F"  # emoticons
        u"\U0001F300-\U0001F5FF"  # symbols & pictographs
        u"\U0001F680-\U0001F6FF"  # transport & map symbols
        u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
        u"\U00002702-\U000027B0"
        u"\U000024C2-\U0001F251"
        u"\U0001f926-\U0001f937"
        u'\U00010000-\U0010ffff'
        u"\u200d"
        u"\u2640-\u2642"
        u"\u2600-\u2B55"
        u"\u23cf"
        u"\u23e9"
        u"\u231a"
        u"\u3030"
        u"\ufe0f"
                           "]+", flags=re.UNICODE)

def getHashtags(rdd_collected):
    list_hashtags = []
    for element in rdd_collected:
        print(element)
        for hashtag in element:
            if hashtag["text"] != None:
                list_hashtags.append(hashtag["text"])
    return list_hashtags

def stripTextAndRemoveEmoji(text):
    stringWithoutEmoji = emoji_pattern.sub(r'', text)
    nameIsBlank = re.search('^\s*$', stringWithoutEmoji)
    if not stringWithoutEmoji or nameIsBlank :
        return "No name"
    return stringWithoutEmoji.strip()

        

def process(time, rdd):
    print("========= %s =========" % str(time))
    if not rdd.isEmpty():
        print(rdd)
        locationDF = rdd.map(lambda tweet: Row(username=stripTextAndRemoveEmoji(tweet['user']['name']), 
                              nb_friends=tweet['user']['friends_count'])).toDF()
        
        locationDF = locationDF.withColumn("username", locationDF["username"].cast(StringType())) \
                               .withColumn("nb_friends", locationDF["nb_friends"].cast(IntegerType()))
        
        locationDF.printSchema()

        locationDF.write.format('jdbc').options(
            url='jdbc:mysql://192.168.33.10/data',
            dbtable='users',
            user='admin',
            password='admin').mode('append').save()
        
        
        rdd_collected = rdd.map(lambda tweet: tweet["entities"]["hashtags"]).collect()
        
        if len(rdd_collected) >= 1:
            hashtags_list = getHashtags(rdd_collected)
            rdd_hashtags = sc.parallelize(hashtags_list)
            if rdd_hashtags.isEmpty() == False:
                hashtagsDF = rdd_hashtags.map(lambda hashtagg: Row(hashtag=hashtagg)).toDF()                        
                hashtagsDF = hashtagsDF.withColumn("hashtag", hashtagsDF["hashtag"].cast(StringType()))       
                hashtagsDF.printSchema()        
                hashtagsDF.write.format('jdbc').options(
                    url='jdbc:mysql://192.168.33.10/data',
                    dbtable='hashtags',
                    user='admin',
                    password='admin').mode('append').save()
                            
                                                       
spark = SparkSession.builder \
        .master("local[2]") \
        .appName("data") \
        .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("ERROR")
ssc = StreamingContext(sc, 10)

directKafkaStream = KafkaUtils.createDirectStream(ssc, ["tweets"], {"metadata.broker.list": "192.168.33.13:9092"})
rdd = directKafkaStream.map(lambda tweet: json.loads(tweet[1]))
rdd.foreachRDD(process)


ssc.start()
ssc.awaitTermination()
