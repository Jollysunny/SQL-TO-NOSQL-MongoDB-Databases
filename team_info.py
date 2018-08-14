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
db = client.fifa

cnx = mysql.connector.connect(user='root', password='',
                              host='127.0.0.1',
                              database='FIFAWORLDCUP')
cursor = cnx.cursor()

print("Getting required data from MySQL")
print("--------------------------------")
query =("SELECT `MatchType`, `MatchDate`, `STADIUM`, `TEAM1`, `TEAM2`, `Team1_Score`, `Team2_Score` FROM `game_info` order by `TEAM1` ASC")

cursor.execute(query)
obj_arr = []
obj_arr1=[]
i=0
j=0
for (MatchType, MatchDate, STADIUM, TEAM1, TEAM2, Team1_Score,Team2_Score) in cursor:
    obj={}

           #subobj['MatchType'] = MatchType
    obj['Team']=TEAM1
    obj['Match_Type'] = MatchType
    obj['STADIUM'] =STADIUM
    obj['TEAM1'] =TEAM1
    obj['TEAM2'] =TEAM2
    obj['Team1_Score'] =Team1_Score
    obj['Team2_Score'] =Team2_Score
    obj_arr.append(obj)
    print(obj_arr)
    j=j+1

    if (i==0):
        obj_arr1.append(obj)
        i=i+1
    print(j)
                #json_acceptable_string = obj_arr1.replace("'", "\"")
dict = json.dumps(obj_arr)
query2 =("SELECT `MatchType`, `MatchDate`, `STADIUM`, `TEAM1`, `TEAM2`, `Team1_Score`, `Team2_Score` FROM `game_info` order by `TEAM2` ASC")

cursor.execute(query2)
for (MatchType, MatchDate, STADIUM, TEAM1, TEAM2, Team1_Score,Team2_Score) in cursor:
    obj={}

           #subobj['MatchType'] = MatchType
    obj['Team']=TEAM2
    obj['Match_Type'] = MatchType
    obj['STADIUM'] =STADIUM
    obj['TEAM1'] =TEAM1
    obj['TEAM2'] =TEAM2
    obj['Team1_Score'] =Team1_Score
    obj['Team2_Score'] =Team2_Score
    obj_arr.append(obj)
    print(obj_arr)
    j=j+1

    if (i==0):
        obj_arr1.append(obj)
        i=i+1
    print(j)
                #json_acceptable_string = obj_arr1.replace("'", "\"")
dict = json.dumps(obj_arr)
cursor.close()
cnx.close()

print("Importing into MongoDB")
print("----------------------")
result = db.fifa.insert_many(obj_arr)
print(result.inserted_ids)
print(result.acknowledged)
print("Total Result entry:")
print(j)
