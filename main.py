# -*- coding: utf-8 -*-

"""
#===============================================
Flock project
File: main.py

A collection of scripts to scrape Twitter and extract networks.
#===============================================
"""

import xGetTwitter as xgt
import json
from tweepy import OAuthHandler
from tweepy import API
from tweepy import Cursor

apiFile = "api_twitter.json"

f = open(apiFile,'r')
keys = json.loads(f.read())
f.close()

auth = OAuthHandler(keys['consumer_key'], keys['consumer_secret'])
auth.set_access_token(keys['access_token'], keys['access_token_secret'])    
api = API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True, compression=True)

keyList=[['%23mooc',20, True, 'en'],
['%23selfcare',7000, True, 'en'],
['%23innovation',7000, True, 'en'],
['global health',7000, True, 'en'],
['financial independence',7000, True, 'en'],
['%23iot future',7000, True, 'en'],
['%40teslamotors',7000, True, 'en'],
['%23driverlesscars OR %23autonomouscars OR %23selfdrivingcars',7000, True, 'en'],
['%23smartcities',7000, True, 'en'],
['%23opendata',7000, True, 'en'],
['%23ceta',7000, True, False],
['%40nest',7000, True, 'en'],
['tesla model 3',7000, True, 'en'],
['electric cars',7000, True, 'en'],
['DIY',7000, True, 'en'],
['%23Dataviz',7000, True, 'en'],
['%23pokemon',7000, True, False],
['%23pokemonGo',7000, True, False],
['data geneva',7000, True, False],
['annecy',7000, True, False],
['infectious diseases',7000, True, 'en'],
['emerging infections',7000, True, 'en'],
['%23Zika OR %23zikavirus OR %23zikathreat',7000, True, 'en'],
['%22functional medicine%22 OR %23FunctionalMedicine',7000, True, 'en'],
['%22precision medicine%22 OR %23PrecisionMedicine',7000, True, 'en'],
['%22systems biology%22 OR %23SystemsBiology',7000, True, 'en'],
['epigenetics OR %23epigenetics',7000, True, 'en'],
['microbiome',7000, True, 'en'],
['%23integrativeMedicine OR "integrative medicine"',7000, True, 'en'],
['autoimmune',7000, True, 'en']]

for item in keyList[:15]:
    print("Scraping: " + item[0])  
    xgt.fromWeb(api,item[0],item[1],item[2],item[3])

"""
#===============================================
Make a graph from several tweet dumb files
import xMakeNet as xmn
xmn.mergeFiles("tmp/")
g = xmn.extract("interim/test-mooc10.txt")
#===============================================
"""
import xMakeNet as xmn
fileStem="/home/mx/ext/autoimmune/160907_autoimmune" 

folderPath = "/media/sf__l-data/_data-in/pip_global/"
xmn.mergeFiles(folderPath)
filePath = "/media/sf__l-data/_data-in/pip_global/bundle_1701-04_global_health.txt"
g = xmn.extract(filePath)

"""
#===============================================
A: Make a graph from a list of ids
B: Unfold network of friends and followers from graph

#===============================================
"""

# INITIATE
from tweepy import OAuthHandler
from tweepy import API
from tweepy import Cursor
import json
import networkx as nx
import xMakeNet as xmn

apiFile = "api_twitter.json"
with open(apiFile) as data_file:    
    keys = json.load(data_file)
data_file.close()
auth = OAuthHandler(keys['consumer_key'], keys['consumer_secret'])
auth.set_access_token(keys['access_token'], keys['access_token_secret'])    
api = API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True, compression=True)

# START EXPLORING

# IF no graph (A)
seedList = ['ciat_','SoilAssociation','CivilEats','foodtank','FAOKnowledge','ifpri','ICRAF','BioversityInt','Cgiar','FAOclimate','DaniNierenberg','CARE'] # make your own list
g = nx.DiGraph()
xmn.addByID(api,g,seedList)

# IF graph (B)
g = nx.read_graphml("try-mx.graphml") # if in file
select = lambda g,n: (not 'ripFlag' in g.node[n] and (g.out_degree(n)<4) and (g.in_degree(n)<6) and ((2*g.in_degree(n) + g.out_degree(n)) >= 7))
seedList = [g.node[n]['id'] for n in g.nodes() if select(g,n)]

# cutoff options
cutOff = lambda u: 1 > 0
cutOff = lambda u: ((u.followers_count) > 0 and (u.friends_count) > 0 and (u.statuses_count) > 5)

f = xmn.unfoldNet(api,g,seedList,200,200,cutOff) #maximum 200! otherwise default to 20

# write graph
fileStem = "follownet"
# fileStem = "/home/mx/ext/autoimmune/160907_autoimmune"
nx.write_graphml(f,fileStem +"_0.graphml", encoding='utf-8', prettyprint=True)
nx.write_graphml(f,fileStem +"_1.graphml", encoding='utf-8', prettyprint=True)
nx.write_graphml(g,fileStem +"_2.graphml", encoding='utf-8', prettyprint=True)
nx.write_graphml(g,fileStem +"_3.graphml", encoding='utf-8', prettyprint=True)   
