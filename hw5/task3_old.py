import tweepy
import csv
import sys
from collections import defaultdict
import random


class MyStreamListener(tweepy.StreamListener):

    def __init__(self):
        tweepy.StreamListener.__init__(self)
        self.n_tweets = int(1)          # the number of tweets got
        self.hold_tweets = defaultdict(list)     # tweets hold
        self.count_tags = defaultdict(int)       # count apperence of tags
        self.probability = float(1)
        # create csv file
        with open(outputFile, "w") as opf:
            csv.writer(opf)



    
    def on_status(self, status):
        # got tweets and its hashtags list, it's a list of dictionary 
        # looks like [ 
        #               {'text':'xxxx1','indeices':[some number]}, 
        #               {'text':'xxxx2','indeices':[some number]},
        #                   .........,
        #               {'text':'xxxx3','indeices':[some number]}
        # ]
        tag_list = status.entities['hashtags']
        # have got n items
        if self.n_tweets <= 100:
            # if have tag
            if len(tag_list) > 0:
                tagss = []          # to save tags in this tweet
                for item in tag_list:
                    tag = item.get('text')
                    if tag is not None:
                        tagss.append(tag)
                        self.count_tags[tag] += 1
                # if have tag
                if len(tagss) > 0:
                    self.hold_tweets[self.n_tweets] = tagss
                    # after update hold_tweets, update csv file
                    sort_count_tag = sorted(self.count_tags.items(), key=lambda k: (-k[1],k[0]))
                    with open(outputFile, 'a', encoding="utf-8") as opf:
                        opf.write("The number of tweets with tags from the beginning: " + str(self.n_tweets) + '\n')
                        ii = 1
                        maxv = sort_count_tag[0][1]
                        for tag, count in sort_count_tag:
                            if count < maxv:
                                ii += 1
                                maxv = count                               
                            if ii > 3:
                                break
                            else:
                                opf.write(tag + " : " + str(count) + '\n')
                        opf.write("\n")


                    self.n_tweets += 1
        elif self.n_tweets > 100:       # after 100 tweets got
            if len(tag_list) > 0:
                # probability to keep next one
                self.probability = 100/self.n_tweets
                if random.random() < self.probability:
                    tagss = []          # to save tags in this tweet
                    for item in tag_list:
                        tag = item.get('text')
                        if tag is not None:
                            tagss.append(tag)
                            self.count_tags[tag] += 1
                    
                    # if have tag
                    if len(tagss) > 0:
                        discard = random.randint(1,100)     # discard index
                        # discard
                        discarding_tweet = self.hold_tweets[discard]
                        for item in discarding_tweet:
                            self.count_tags[item] -= 1
                            if self.count_tags[item] == 0:
                                del self.count_tags[item]
                        self.hold_tweets[self.n_tweets] = tagss
                        # after update hold_tweets, update csv file
                        sort_count_tag = sorted(self.count_tags.items(), key=lambda k: (-k[1],k[0]))
                        with open(outputFile, 'a', encoding="utf-8") as opf:
                            opf.write("The number of tweets with tags from the beginning: " + str(self.n_tweets) + '\n')
                            ii = 1
                            maxv = sort_count_tag[0][1]
                            for tag, count in sort_count_tag:
                                if count < maxv:
                                    ii += 1
                                    maxv = count                               
                                if ii > 3:
                                    break
                                else:
                                    opf.write(tag + " : " + str(count) + '\n')
                            opf.write("\n")
                        # update n_tweets
                        self.n_tweets += 1

                                
        

API_key = '9zs0N7H0wEfGzjgPaRfl9dfMA'
API_secret_key = '81R9L015At0xGWq3RMmf97QGIO4zJWs76DdXWuIjNOZEsC7v04'
Access_token = '1275663969521414147-PKN5xR5BMvZ1CNtOj3Nu2GJ14ozr7S'
Access_token_secret = '7tejYaGNif7dfKrnt2y71VKGbcWHdEtzN1qOqszydWdQr'



#filter_list = ['COVID19', 'StayAtHome', 'Trump', 'CoronaVirus', 'Pandemic']
filter_list = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o',
'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z']


outputFile = 'task3output.csv'
#outputFile = sys.argv[2]


# Step 1: Creating a StreamListener
myStreamListener = MyStreamListener()

# Step 2: Creating a Stream
auth = tweepy.OAuthHandler(API_key, API_secret_key)
auth.set_access_token(Access_token, Access_token_secret)
""" api = tweepy.API(auth)
api.update_status('tweepy + oauth!') """
myStream = tweepy.Stream(auth=auth, listener=myStreamListener)

# Step 3: Starting a Stream
myStream.filter(track=['a'], languages=['en'])       # track=['python']



