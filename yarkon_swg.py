import datetime
from pymongo import MongoClient
import asyncio
import urllib.parse
from deepdiff import DeepDiff as df
import sys,getopt
import pandas as pd

n = len(sys.argv)
argv = sys.argv[1:2]
for idx, arg in enumerate(sys.argv):
    if idx == 0 :
        dt = datetime.datetime.today() - datetime.timedelta(days = 1 )
        hr = 00
    if idx==1:
        try:
            dt = datetime.datetime.strptime(arg, '%d-%m-%y').date()
        except ValueError as ve:
            raise ValueError(ve)


#filter_from_time_iso = filter_from_time.isoformat()
#filter_to_time_iso = filter_to_time.isoformat()

username = urllib.parse.quote_plus('yarkon_app')
password = urllib.parse.quote_plus('byrrrhhj7amX2jXZ')

username_p = urllib.parse.quote_plus('oferelzam')
password_p = urllib.parse.quote_plus('n8VzbArS3rFxApEU')

string_conn_yarkon = ("mongodb+srv://%s:%s@p81-staging.2kvan.mongodb.net" % (username, password))
client_yarkon = MongoClient(string_conn_yarkon, tls=True,tlsAllowInvalidCertificates=True)
db_yarkon = client_yarkon["p81-staging"]

string_conn_snow = ("mongodb+srv://%s:%s@p81-production.jrtfl.mongodb.net" % (username_p, password_p))
client_snow = MongoClient(string_conn_snow, tls=True, tlsAllowInvalidCertificates=True)
db_snow = client_snow["p81-production"]

coll_yarkon_warn = db_yarkon["SWGEvents"]
coll_snow_warn = db_snow["EventsSWGInQueue"]

coll_yarkon_block = db_yarkon["SWGEvents"]
coll_snow_block = db_snow["EventsSWGInQueue"]

async def mongo_count_events():

    print("WARN EVENT  YARKON  SNOW DIFF")

    for i in range(0, 23):
        filter_from_time = datetime.datetime.combine(dt, datetime.time(i))
        filter_to_time = datetime.datetime.combine(dt, datetime.time(i + 1))
        filter_from_time_iso = filter_from_time.isoformat()
        filter_to_time_iso = filter_to_time.isoformat()

        filter_yarkon = ({"$and": [{'eventTime': {"$gte": filter_from_time, "$lte": filter_to_time}},
                                 {'fqcn': 'com.p81.yarkon.agent.events.TrafficSwgUrlWarn'}]})

        filter_snow = ({"$and": [{'serverTimestamp': {"$gte": filter_from_time, "$lte": filter_to_time}},
                                 {'eventName': 'traffic|swg|url_warn'}]})

        rows_yarkon_warn = coll_yarkon_warn.count_documents(filter_yarkon)
        rows_snow_warn = coll_snow_warn.count_documents(filter_snow)
        diff_count = rows_yarkon_warn - rows_snow_warn
        text = 'TIME ---' + str(datetime.time(i+1))  + '  '  + '  ' + str(rows_yarkon_warn) + '  ' + str(rows_snow_warn) + \
               '  ' + str(diff_count)
        print(text)


    print("START BLOCK EVENT  YARKON  SNOW DIFF")
    i=0
    for i in range(0, 23):

        filter_from_time = datetime.datetime.combine(dt, datetime.time(i))
        filter_to_time = datetime.datetime.combine(dt, datetime.time(i + 1))
        filter_from_time_iso = filter_from_time.isoformat()
        filter_to_time_iso = filter_to_time.isoformat()

        filter_yarkon = ({"$and": [{'eventTime': {"$gte": filter_from_time, "$lte": filter_to_time}},
                   {'fqcn': 'com.p81.yarkon.agent.events.TrafficSwgUrlBlock'}]})


        filter_snow = ({"$and": [{'serverTimestamp': {"$gte": filter_from_time, "$lte": filter_to_time}},
                                {'eventName': 'traffic|swg|url_block'}]})

        rows_yarkon_block = coll_yarkon_block.count_documents(filter_yarkon)
        rows_snow_block = coll_snow_block.count_documents(filter_snow)
        diff_count = rows_yarkon_block - rows_snow_block
        text = 'TIME ---' + str(datetime.time(i + 1) ) + '  ' + str(rows_yarkon_block) + '  ' + str(rows_snow_block) + \
                   '  ' + str(diff_count)
        print(text)


async def mongo_compare_warn_event():

    filter_from_time = datetime.datetime.combine(dt, datetime.time.min)
    filter_to_time = datetime.datetime.combine(dt, datetime.time.max)

    filter_yarkon = ({"$and": [{'eventTime': {"$gte": filter_from_time, "$lte": filter_to_time}},
               {'fqcn': 'com.p81.yarkon.agent.events.TrafficSwgUrlWarn'}]})

    struc_yarkon_warn = coll_yarkon_warn.find_one(filter_yarkon)

    if isinstance(struc_yarkon_warn, dict) :
        struc_yarkon_warn_all = struc_yarkon_warn #| struc_yarkon_warn['common']
        event_id = struc_yarkon_warn['eventId']
        filter_id = {'eventId': {"$eq": event_id}}
        struc_snow_warn = coll_snow_warn.find_one(filter_id)
        if isinstance(struc_snow_warn, dict):
            diff = df(struc_yarkon_warn_all, struc_snow_warn)
            if 'dictionary_item_removed' in  diff.keys():
                str_remove = str(diff['dictionary_item_removed']).replace('root', '').replace('[', '').replace(']','').replace('common','')
            else:
                str_remove= ''
            if 'dictionary_item_removed' in  diff.keys():
                str_add = str(diff['dictionary_item_added']).replace('root', '').replace('root', '').replace('[','').replace(']','').replace('common','')
            else:
                str_add = ''
            if 'values_changed' in diff.keys():
                str_values_changed = str(diff['values_changed']).replace('root', '').replace('root', '').replace('[','').replace(']', '').replace('common', '')
            else:
                str_values_changed = ''
        else:
            diff = str(event_id) + ' ' + 'Event Id Not Exists in Snow'
            str_remove, str_add ,str_values_changed= '','',''
    else:
        diff = 'No rows found in Yarkon'
        str_remove, str_add, str_values_changed = '','',''
        event_id = 'EVENT NOT FOUND'


    text = "quality comparing for warn Event id  ".title()  + str(event_id).title() +  ' \n'\
           "field not exists in  snowplow  -  "  + str_remove +  ' \n' + \
           "field not  exists in yarkon  -  "  + str_add +  ' \n' + \
           "values not equal -  " + str_values_changed
    print(text)

async def mongo_compare_block_event():
    filter_from_time = datetime.datetime.combine(dt, datetime.time.min)
    filter_to_time = datetime.datetime.combine(dt, datetime.time.max)

    filter_yarkon = ({"$and": [{'eventTime': {"$gte": filter_from_time, "$lte": filter_to_time}},
                      {'fqcn': 'com.p81.yarkon.agent.events.TrafficSwgUrlBlock'}]})

    struc_yarkon_block = coll_yarkon_block.find_one(filter_yarkon)

    if isinstance(struc_yarkon_block, dict) :
        event_id = struc_yarkon_block['eventId']
        filter_id = {'eventId': {"$eq": event_id}}
        struc_snow_block = coll_snow_block.find_one(filter_id)
        if isinstance(struc_snow_block, dict):
            struc_yarkon_block_all = struc_yarkon_block #| struc_yarkon_block['common']
            diff = df(struc_yarkon_block_all, struc_snow_block)
            if 'dictionary_item_removed' in  diff.keys():
                str_remove = str(diff['dictionary_item_removed']).replace('root', '').replace('[', '').replace(']','').replace('common','')
            else:
                str_remove=''
            if 'dictionary_item_removed' in  diff.keys():
                str_add = str(diff['dictionary_item_added']).replace('root', '').replace('root', '').replace('[','').replace(']','').replace('common','')
            else:
                str_add =''
            if 'values_changed' in diff.keys():
                str_values_changed = str(diff['values_changed']).replace('root', '').replace('root', '').replace('[','').replace(']', '').replace('common', '')
            else:
                str_values_changed = ''
        else:
            diff = str(event_id) #+ ' ' + 'Event Id Not Exists in Snow'
            str_remove, str_add ,  str_values_changed= '','',''
    else:
        diff = 'No rows found in Yarkon'
        event_id = 'EVENT NOT FOUND'
        str_remove, str_add,str_values_changed = '','',''


    #str_remove = str(diff['dictionary_item_removed']).replace('root','').replace('[','').replace(']','').replace('common','')
    #str_add = str(diff['dictionary_item_added']).replace('root','').replace('root','').replace('[','').replace(']','').replace('common','')

    text = "quality comparing for BLOCK Event id  ".title() + str(event_id) +  ' \n'\
           "field not exists in  snowplow  -  " + str_remove + ' \n' + \
           "field not  exists in yarkon  -  " + str_add + ' \n' + \
           "values not equal in event -  " + str_values_changed
    print(text)

async def main():
    print( str(dt))
    await mongo_count_events()
    print('END SWG COUNTS EVENT')
    await mongo_compare_warn_event()
    #await mongo_compare_block_event()

if __name__ == "__main__":                                                                                                                      l
    asyncio.run(main())
