#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Aug 11 18:00:11 2018

@author: harshsaini
"""

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Aug 11 02:16:10 2018

@author: harshsaini
"""

import mysql.connector
import json
import pymongo
from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017/')
db = client.fifa2

cnx = mysql.connector.connect(user='root', password='',
                              host='127.0.0.1',
                              database='FIFAWORLDCUP')
cursor = cnx.cursor()

print("Getting required data from MySQL")
print("--------------------------------")
query =("SELECT `PName`, `Team`, `Position`, `GOALS_SCORED`, `STADIUM`, `opp_team`,`Goaltype` FROM `player_data1`")
cursor.execute(query)
obj_arr = []
obj_arr1=[]
i=0
j=0
arr=[]
subobj = {}
for (PName, Team, Position, GOALS_SCORED,STADIUM,opp_team,Goaltype) in cursor:
    obj={}

           #subobj['MatchType'] = MatchType
    obj['PName'] = PName
    obj['Team'] =Team
    obj['Position'] =Position
    obj['GOALS_SCORED'] =GOALS_SCORED

    obj['STADIUM'] =STADIUM
    obj['OPP_TEAM']=opp_team
    obj['GoalType']=Goaltype

    subobj['opp_team']=opp_team
    subobj['STADIUM'] = STADIUM
    subobj['Fname'] = opp_team
    subobj['Goaltype'] = Goaltype
    arr.append(subobj)
    obj['Game:']=arr
    obj_arr.append(obj)
    print(obj_arr)
    j=j+1

    if (i==0):
        obj_arr1.append(obj)
        i=i+1
    print(j)
                #json_acceptable_string = obj_arr1.replace("'", "\"")


dict = json.dumps(obj_arr)


print("Importing into MongoDB")
print("----------------------")
result = db.fifa2.insert_many(obj_arr)
print(result.inserted_ids)
print(result.acknowledged)
print("Total Result entry:")
print(j)
