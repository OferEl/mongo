import boto3 as b3
import pandas as pd
from pandas.io.json import json_normalize
import datetime
import pymongo as mongo
from pymongo import MongoClient
import asyncio
import urllib.parse
from deepdiff import DeepDiff as df


#first landing of swg events is the EventsSWGInQueue
#p81-production#p81-staging#p81-integration

filter_time = datetime.datetime.now() - datetime.timedelta(hours=5)
filter_time_iso = filter_time.isoformat()
filter_eventTime = {'common.eventTime': {"$gte": filter_time_iso } }

async def mongo_worm_event():
    ##cluster
    username = urllib.parse.quote_plus('ofer')
    password = urllib.parse.quote_plus('iDsuYjHbBjEl77s1')

    string_conn_yarkon = ("mongodb+srv://%s:%s@p81-integration.z8ocy.mongodb.net" % (username, password))
    client_yarkon = MongoClient(string_conn_yarkon, tls=True, tlsAllowInvalidCertificates=True)
    db_yarkon = client_yarkon["p81-integration"]
    coll_yarkon_block = db_yarkon["YarkonTrafficSwgUrlworm"]

    #string_conn_snow = ("mongodb+srv://%s:%s@p81-production.jrtfl.mongodb.net" % (username, password))
    string_conn_snow = ("mongodb+srv://%s:%s@p81-integration.z8ocy.mongodb.net" % (username, password))
    client_snow = MongoClient(string_conn_snow, tls=True, tlsAllowInvalidCertificates=True)
    db_snow = client_snow["p81-integration"]
    coll_snow_block = db_snow["SWGEvents"]

    rows_yarkon_block = coll_yarkon_block.count_documents(filter_eventTime)
    rows_snow_block = coll_snow_block.count_documents(filter_eventTime)

    text = '-' 'yarkon Traffic Swg Url Block is -' + str(rows_yarkon_block) + \
           "---------" + 'snow Traffic Swg Url Block is -' + str(rows_snow_block)
    print(text)


async def mongo_block_event():
    print('mongo_block_event start at' + '------'+str(datetime.datetime.now()))
    ##cluster
    username = urllib.parse.quote_plus('ofer')
    password = urllib.parse.quote_plus('iDsuYjHbBjEl77s1')
    #mongodb+srv://<username>:<password>@p81-production.jrtfl.mongodb.net/myFirstDatabase?retryWrites=true&w=majority
    string_conn_yarkon = ("mongodb+srv://%s:%s@p81-integration.z8ocy.mongodb.net" % (username, password))
    client_yarkon = MongoClient(string_conn_yarkon, tls=True,tlsAllowInvalidCertificates=True)
    db_yarkon = client_yarkon["p81-integration"]
    coll_yarkon_block = db_yarkon["YarkonTrafficSwgUrlBlock"]

    #mongodb+srv://:@p81-production.jrtfl.mongodb.net/myFirstDatabase?retryWrites=true& w = majority
    #mongodb+srv://:@p81-production.jrtfl.mongodb.net/myFirstDatabase?retryWrites=true& w = majority

    #string_conn_snow =("mongodb+srv://%s:%s@p81-integration.z8ocy.mongodb.net" % (username, password))
    string_conn_snow = ("mongodb+srv://%s:%s@p81-production.jrtfl.mongodb.net" % (username, password))
    client_snow = MongoClient(string_conn_snow, tls=True, tlsAllowInvalidCertificates=True)
    db_snow = client_snow["p81-production"]
    coll_snow_block = db_snow["EventsSWGInQueue"]

    rows_yarkon_block   = coll_yarkon_block.count_documents(filter_eventTime)
    rows_snow_block = coll_snow_block.count_documents(filter_eventTime)

    text = '  ' +'yarkon Traffic Swg Url Block count = ' + str(rows_yarkon_block) + \
           " ---------"+'  snow Traffic Swg Url Block count =' + str(rows_snow_block)
    print(text)

    struc_yarkon_block = coll_yarkon_block.find_one(filter_eventTime)
    struc_snow_block = coll_yarkon_block.find_one(filter_eventTime)

    diff = df(struc_yarkon_block, struc_snow_block)
    print(str(diff))

    #if pdf_yarkon_block.equals(pdf_snow_block):
        #print(df + "equals")
    #else:
        #print(df + "ubequals")



async def main():
    await mongo_block_event()
    #await mongo_worm_event()
    print('end')

if __name__ == "__main__":
    asyncio.run(main())
