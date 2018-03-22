# -*- coding: utf-8 -*-
"""===============================================
xMakeNet()
Python 3.5
==============================================="""
import json
import time
import os
import networkx as nx
from datetime import datetime, date, timedelta

"""
#===============================================
Merging files with tweets
mergeFiles(dirPath) = mergeFiles("data/")
----
The serie of file are kept by themselves in the dirPath.
A single file is written to the dirPath with "bundle_" followed by name of first file.
#===============================================
"""
# import os import json import time

def mergeFiles(dirPath):
    # read filenames of files in folder
    fileList = os.listdir(dirPath)
    fileList.sort(key=str.lower,reverse=True)
    master = [json.loads(line.strip()) for line in open(dirPath + fileList[0], 'r')]
    # process all files
    for file in fileList:
    #for file in fileList[1:]: // only if bundle file exists
        print("processing:" + file)
        temp =  [json.loads(line.strip()) for line in open(dirPath + file, 'r')]
        earlist = time.strptime(master[-1]['created_at'],'%a %b %d %H:%M:%S +0000 %Y')
        cutOff = None
        for idx,tweet in enumerate(temp):
            tweetDate = time.strptime(tweet['created_at'],'%a %b %d %H:%M:%S +0000 %Y')
            if tweetDate < earlist:
                print("earlier:" + str(idx) + " : " + file)
                cutOff = idx
                break
        if cutOff != None:
            print("cutOff:"+ str(idx) + " : " + file)
            del temp[:idx]
            master.extend(temp)
    # write collection of tweets to a new file            
    with open(dirPath + 'bundle_' + fileList[0], 'w') as outfile:
        for item in master:
            outfile.write(json.dumps(item))
            outfile.write("\n")  
            
"""
#===============================================
extract(filePath) = extract("data/test-mooc10.txt")
Make graph of twitter accounts based on tweets, retweets and mentions from a corpus of tweets.
=======
Takes a txt file and writes a new file with "_extract.graphml" appended.
Function return networkx graph.

Inspiration
Making a graph of retweets and mentions: https://github.com/computermacgyver/twitter-python/blob/master/data2metions_retweet_network.py
#===============================================
"""

def parseTweet(t,g):
    #is it a retweet?
    reTweet = False
    startRT = t['text'].startswith('RT')     
    if 'retweeted_status' in t or startRT:
        reTweet = True
        print("istrue")
    else:
        reTweet = False
        print("isfalse")
    #process user
    aRef = t["user"]
    aID = aRef["id_str"]
    if not aID in g:
        # tdiff = datetime.now() - aRef['created_at']
        attrDic = {
            'id':aRef['id'],
            'screen_name':aRef['screen_name'],
            'lang':aRef['lang'],
            'location':aRef['location'],
            #'created_at':aRef['created_at'].strftime('%Y-%m-%d %H:%M:%S'),
            #'age_days': tdiff.days,
            'friends_count':aRef['friends_count'],
            'followers_count': aRef['followers_count'],
            'statuses_count': aRef['statuses_count'],
            #'freq': (round(float(aRef['statuses_count']) / (tdiff.days+1),2)),
            'favourites_count': aRef['favourites_count'],
            'batch_tweets':0,
            'full':True # indicate if node has all info
            } 
        if 'time_zone' in aRef:
            attrDic['time_zone'] = aRef['time_zone']
        g.add_node(aID,attrDic)
    if not reTweet:
        g.node[aID]['batch_tweets'] += 1
    sRef = aRef # variables being reused
    sID = aID    

    if 'retweeted_status' in t and 'user' in t['retweeted_status']:
        aRef = t['retweeted_status']['user']
        aID = aRef['id_str']
        # add retweeted user
        # if not 'batch_tweets' in g.node[aID]:
        if not aID in g:
            # tdiff = datetime.now() - aRef['created_at']
            attrDic = {
                'id':aRef['id'],
                'screen_name':aRef['screen_name'],
                'lang':aRef['lang'],
                'location':aRef['location'],
                #'created_at':aRef['created_at'].strftime('%Y-%m-%d %H:%M:%S'),
                #'age_days': tdiff.days,
                'friends_count':aRef['friends_count'],
                'followers_count': aRef['followers_count'],
                'statuses_count': aRef['statuses_count'],
                #'freq': (round(float(aRef['statuses_count']) / (tdiff.days+1),2)),
                'favourites_count': aRef['favourites_count'],
                'batch_tweets':0,
                'full':True # indicate if node has all info               
                } 
            if 'time_zone' in aRef:
               attrDic['time_zone'] = aRef['time_zone']
            g.add_node(aID,attrDic)
            # check if edge exist or create
            if not g.has_edge(aID,sID):
                g.add_edge(aID,sID,weight=0, batch_mentions=0, batch_retweets=0)
            else:
                print("edge repeated" + str(sID) + str(aID))    
            # update attributes
            g[aID][sID]['weight'] += 1 
            g[aID][sID]['batch_retweets'] += 1 

    if not reTweet:
        # add tweet["entities"]["user_mentions"][*]["screen_name"]
        if "entities" in t and "user_mentions" in t["entities"]:
            users=t["entities"]["user_mentions"]
            for u in users:
                aRef = u
                aID=u["id_str"]
                print('> mention')
                if not aID in g:
                    attrDic = {
                        'id':aRef['id'],
                        'screen_name':aRef['screen_name'],
                        'batch_tweets':0,
                        'full':False # indicate if node has all info
                        } 
                    g.add_node(aID,attrDic)
                # add edge
                # check if edge exist or create
                if not g.has_edge(aID,sID):
                    g.add_edge(aID,sID,weight=0, batch_mentions=0, batch_retweets=0)
                else:
                    print("edge repeated" + str(sID) + str(aID))    
                # update attributes
                g[aID][sID]['weight'] += 1 
                g[aID][sID]['batch_mentions'] += 1 

# output is saved in same location with '.graphml' extension
def extract(filePath):
    filename, file_extension = os.path.splitext(filePath)
    #import data
    tag_data = [json.loads(line.strip()) for line in open(filePath, 'r')]
    # create graph from data
    g = nx.DiGraph()
    for tweet in tag_data:
        parseTweet(tweet,g)
        # delete Nonetype so it does not create error in write
    for node in g.node:
        deleteList = []
        for attrib in g.node[node]:
            if type(g.node[node][attrib]) == type(None):
                deleteList.append(attrib)
        for item in deleteList:
            del g.node[node][item]
    # write output file
    nx.write_graphml(g,filename + '_extract.graphml', encoding='utf-8', prettyprint=True)
    return g
    

"""
#===============================================
addUser and addEdge is used by the following two modules

#===============================================
"""
def addUser(u,g):
    #print("addUser: " + u.screen_name)
    nodeID = str(u.id)
    g.add_node(nodeID)
    g.node[nodeID]['id'] = u.id
    g.node[nodeID]['screen_name'] = u.screen_name
    g.node[nodeID]['lang'] = u.lang
    g.node[nodeID]['location'] = u.location
    if 'time_zone' in g.node[nodeID]:
        g.node[nodeID]['time_zone'] = u.time_zone
    g.node[nodeID]['created_at'] = u.created_at.strftime('%Y-%m-%d %H:%M:%S')
    tdiff = datetime.now() - u.created_at
    g.node[nodeID]['age_days'] = tdiff.days
    g.node[nodeID]['friends_count'] = u.friends_count
    g.node[nodeID]['followers_count'] = u.followers_count
    g.node[nodeID]['statuses_count'] = u.statuses_count    
    g.node[nodeID]['freq'] = round(float(u.statuses_count) / (tdiff.days+1),2)
    g.node[nodeID]['favourites_count'] = u.favourites_count
    

def addEdge(u1id,u2id,g):
    if not g.has_edge(u1id,u2id):
        g.add_edge(u1id,u2id)
    else:
        print("edge repeated: " + str(u1id) + str(u2id))
"""
#===============================================
addByID(api,g,seedList)

Takes a list of ids and add them to a graph with attributes.
#===============================================
"""
def addByID(api,g,seedList):
    exceptList = []
    for idx,seed in enumerate(seedList):
        try:
            originID = str(seed)
            print("start: " + str(idx)+" < " + originID)
            originInfo = api.get_user(originID)
            addUser(originInfo,g)
            print("end: " + str(idx))
        except:
            print(">>>>ERROR " + originID)
            exceptList.append(originID)
            continue   
    print("Except: " + str(len(exceptList)) +" > " + str(exceptList))              
"""
#===============================================
unfoldNet(api,g,seedList,depthFollowers,depthFriends,filterFunc)

Takes a list of ids which is already in the graph and add their followers and friends to graph.

----Example
cutOff = lambda u: ((u.followers_count) > 2 and (u.friends_count) > 3)
xmn.unfoldNet(api,g,seedList[:5],200,200,cutOff)

----Example of how to make seedList from graph

select = lambda g,n: (not 'ripFlag' in g.node[n] and ((2*g.in_degree(n) + g.out_degree(n)) >= 3))
seedList = [g.node[n]['id'] for n in g.nodes() if select(g,n)]

# ripFlag indicate if a user have been ripped for followers and friends
#===============================================
"""
def unfoldNet(api,g,seedList,depthFollowers,depthFriends,filterFunc):
    exceptList = []    
    for idx,seed in enumerate(seedList):
        originID = str(seed)
        print("start: " + str(idx) + " < " + str(originID))
        try:
            listFollow = api.followers(id=originID, count=depthFollowers)
            for user in listFollow:
                nodeName = str(user.id)
                if filterFunc(user):
                    if not g.has_node(nodeName):
                        addUser(user,g)
                    addEdge(nodeName,originID,g)
            listFriend = api.friends(id=originID, count=depthFriends)
            for user in listFriend:
                nodeName = str(user.id)
                if filterFunc(user):
                    if not g.has_node(nodeName):
                        addUser(user,g)
                    addEdge(originID,nodeName,g)
            g.node[originID]['ripFlag'] = 1
        except:
            g.node[originID]['ripFlag'] = -1
            exceptList.append(str(originID))
            continue 
        print("end: " + str(idx))
    print("Except: " + str(len(exceptList)) + " : " + str(exceptList))
    return g
