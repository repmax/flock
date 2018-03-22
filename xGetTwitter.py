# -*- coding: utf-8 -*-
"""===============================================
xGetTwitter()
Python 3.5
_______________________________________________
DESCRIPTION
fromFile(dump_file_txt, remove_retweets, lang_filter ): 
- open and filter a dump file into a dataframe and save as a csv
fromWeb(api_file, search_query, max_tweets, remove_retweets, lang_filter):
- send and grap a dump of a twitter query
- filter the dump into a dataframe and save as a csv.
_______________________________________________
EXAMPLES
# If needed to set path:
import sys sys.path.append(r"d:\_e_shed\_dev\mineTwitter")

from getTwitter import *
fromWeb('api_twitter.txt','%23data %23geneve',109, True, 'en')
fromFile('innovation-10.txt', True, 'en')
_______________________________________________
ASSUMING
auth file is in working directory.
removal of duplicates (25 first characters) are assuming that column of text is named 'text'
_______________________________________________
INSPIRATION
http://blog.coderscrowd.com/twitter-hashtag-data-analysis-with-python/
==============================================="""

from tweepy import OAuthHandler
from tweepy import API
from tweepy import Cursor
import json
import pandas as pd
import numpy as np
import time
import os

def fromWeb(api, qStr, MAX_TWEETS, remove_retweet, filter_lang):
    fileStem = str(time.strftime("%y%m%d_%H%M%S")) + '_' + "".join(c for c in (qStr.replace(' ','_')) if c.isalnum() or c in ['-', '_']).rstrip()
    tag_data = rip(api, fileStem, qStr, MAX_TWEETS)
    dump2clean(tag_data, fileStem, remove_retweet, filter_lang)

def fromFile(filePath, remove_retweet, filter_lang):
    tag_data = loadTxtList(filePath)
    filename, file_extension = os.path.splitext(filePath)
    fileList = filename.split('_dump_')
    fileStem = fileList[0]
    dump2clean(tag_data, fileStem, remove_retweet, filter_lang)

def rip(api, fileStem, qStr, MAX_TWEETS):
    # send request
    rawOutput = Cursor(api.search, q=qStr).items(MAX_TWEETS)
    tag_data = []
    # seems to require that the 'tmp-getT..txt' is created beforehand
    fileTmp = fileStem + '_dump_' + '.txt'
    with open(fileTmp, 'w') as outfile:
        for item in rawOutput:
            tag_data.append(item._json)
            # write txtList
            outfile.write(json.dumps(item._json))
            outfile.write("\n")
    fileTweets = fileStem + '_dump_' + str(len(tag_data)) + '.txt'
    os.rename(fileTmp, fileTweets)
    return tag_data

# LOAD DATA FROM DUMP FILE
def loadTxtList(filePointer):
    # Start with next line if tweets has been saved, remember to import functions
    # tag_data = [line.strip() for line in open("/home/mm/_workspace/1462601340.94_iot.txt", 'r')]
    # above gives error, needs addition of json.loads
    tag_data = [json.loads(line.strip()) for line in open(filePointer, 'r')]
    return tag_data

def dump2clean(tag_data, fileStem, remove_retweet = True, filter_lang='en'):
    # CLEAN DUMP FILE FOR RT,LANG, MISSING TEXT
    tag_data = [x  for x in tag_data if isinstance(x['text'],str)]
    if filter_lang:
        tag_data = [x  for x in tag_data if (x['lang'] == filter_lang)]
    if remove_retweet:
        tag_data = [x  for x in tag_data if (x['text'].startswith('RT') != True)]
	
    # CREATE DATAFRAME WITH SELECTED COLUMNS AND SAVE TO CSV
    tweets = pd.DataFrame()
    # extract columns
    def getThem(pointerMain, keyList):
        pointer2List = pointerMain
        for y in keyList[:-1]:
            try:
                pointer2List = pointer2List[y]
            except:
                return np.nan
        #check if empty list
        try:
            if pointer2List:
                aList = []
                for x in pointer2List:
                    aList.append(str(x[keyList[-1]]))
                return ', '.join(aList)
            else:
                return np.nan
        except:
            return np.nan
    def catch(func, handle=lambda e : e, *args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            return np.nan
    level1 = [
        'created_at',
        'favorite_count',
        'favorited',
        'geo',
        'id',
        'source',
        'text',
        'lang',
        'retweet_count'
        ]
    level2 = [
        ['user','description'],
        ['user','screen_name'],
        ['user','followers_count'],
        ['user','friends_count'],
        ['user','id'],
        ['user','location'],
        ['user','created_at'],
        ['user','statuses_count'],
        ['place','full_name']
        ]
    levelxList = [
        ['entities','hashtags','text'],
        ['entities','urls','display_url'],
        ['entities','user_mentions','id'],
        ['user', 'entities', 'url', 'urls', 'display_url']
        ]
    for x in level1:
        tweets[x] = [catch(lambda : y[x]) for y in tag_data]

    for x in level2:
        tweets[x[0]+'_'+ x[1]] = [catch(lambda : y[x[0]][x[1]]) for y in tag_data]

    for x in levelxList:
        tweets[x[0]+'_'+ x[-2]] = [getThem(y,x) for y in tag_data]
    # transform fields
    ## lamda or external function er possible but not needed here
    ## Apply, map return a list which has to be assigned. They do not modify values in object.
    tweets['created_at'] = [time.strftime('%Y-%m-%d %H:%M:%S', time.strptime(x,'%a %b %d %H:%M:%S +0000 %Y')) for x in tweets['created_at']]
    tweets = tweets.replace('',np.nan, regex=True)
    tweets = tweets.fillna(value=np.nan)
    tweets = tweets.replace({'\r': ''}, regex=True)
    # drop duplicates based on first 25 characters in 'text' column keeping the oldest post (can only be done now when it is a dataframe)
    dup_randomnized = 'dup_test_ixwi'
    tweets[dup_randomnized] = [x[:25] for x in tweets['text']]
    tweets = tweets.drop_duplicates(subset=dup_randomnized, keep='last')
    tweets = tweets.drop(dup_randomnized, axis=1)
    #write DataFrame
    tweets.to_csv(fileStem + '_clean_' + str(len(tweets)) +'.csv', encoding='utf-8')
    # the end
