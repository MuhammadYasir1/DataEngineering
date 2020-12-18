# -*- coding: utf-8 -*-
"""
Created on Thu Dec 16 12:40:04 2020

@author: yasir
"""

import datetime
from pymongo import MongoClient
import traceback
#from EasyTaxime import getMEDB, returnDate, formatObjectID#, setLogger# calculatedistance, truncateValue, checkListValue, \
    #checkValue, checkFloat, checkFloatZero
from functions import returnDate,getMEDB,setLogger,formatObjectID
import logging
import pandas as pd
import os


# Connecting to a ODBC
try:
    cnx_local = getMEDB()
    cursor_local = cnx_local.cursor()
    setLogger(r'C:\Users\dell\Desktop\Drivers Face Verification ETL')
except Exception as e:
    print('didnt work')

# Making a connection with Mongo
try:
    
    logging.debug("Starting now")
    connection = MongoClient('mongodb://127.0.0.1:27017', readPreference='secondaryPreferred')
    drivers_face_verification_data = connection['easytaxi_v2_live']['driver_photos']
except Exception as e:
    print(e)

num_days = 2
startdate = returnDate(-num_days + 1)
enddate = returnDate(0)
print (startdate)


count = 0
list_attempts = []
sql_statement = []

# OBBC COLLECTION FILE PARSER
def storeDataInList(items, isAdded):
    global count
    if formatObjectID(str(items['_id'])) not in list_attempts:
        try:
            if count % 50000 == 0:
                print ("Entries Started")
                print (items['created_at'])
                print (datetime.datetime.utcnow())
            count += 1

            # Estimated Values
            verification_attempts = {
                'driver_id': None,
                'status': None,
                'score': None,
                'created_at': None
            }

            if 'driver_id' in items:
                verification_attempts['driver_id'] = items['driver_id']
            if 'status' in items:
                verification_attempts['status'] = items['status']
            if 'score' in items:
                verification_attempts['score'] = items['score']
            if 'created_at' in items:
                verification_attempts['created_at'] = items['created_at']

            sql_statement.append([
                    formatObjectID(str(items['_id'])),
                    str(verification_attempts['driver_id']),
                    str(verification_attempts['status']),
                    str(verification_attempts['score']),
                    verification_attempts['created_at'] + datetime.timedelta(hours=3)
                ])

            list_attempts.append(formatObjectID(str(items['_id'])))
            if count % 50000 == 1:
                print ("Entries Finished")
                print (datetime.datetime.utcnow())
            items = {}

        except Exception as e:
            logging.exception(e)
            traceback.print_exc()
            logging.exception(formatObjectID(str(items['_id'])))


#Main Loop
done = False
while not done:
    db_data = drivers_face_verification_data.find({"created_at": {"$gte": startdate - datetime.timedelta(hours=3)}},
                                            batch_size=1000)
    print ("Loop Starting", datetime.datetime.utcnow())
    for items in db_data:
        storeDataInList(items, 1)
    print ("Loop Ending", datetime.datetime.utcnow())
    done = True

#Write the Driver Offers to CSV File
filename = r"C:\Users\dell\Desktop\Drivers Face Verification ETL\Data"+ str(datetime.datetime.now() - datetime.timedelta(1))[0:10] +".csv"
data_DF = pd.DataFrame(sql_statement)
print (data_DF.shape)
print (data_DF.columns)
print (data_DF.columns.values)
data_DF.to_csv(filename, columns=[0, 1, 2, 3, 4], index=False, header=False)

print( "File Created", datetime.datetime.utcnow())

#Removing Previous Data from DB
print ("Data Deletion - Starting Now", datetime.datetime.utcnow())
cursor_local.execute("Delete from etbi.dbo.face_verification_data where created_at >= '" + startdate.strftime("%Y-%m-%d %H:%M:%S") + "' ")
cnx_local.commit()
print ("Data Deletion - Ending Now", datetime.datetime.utcnow())

#Inserting New File into the DB
print ("Database Insertion - Starting Now", datetime.datetime.utcnow())
QueryCMD = "BULK INSERT etbi.dbo.face_verification_data FROM '"+filename+"' WITH  ( FIRSTROW=1, FIELDTERMINATOR = ',' )"
cursor_local.execute(QueryCMD)
cnx_local.commit()
print ("Bulk Insert - Finished", datetime.datetime.utcnow())
cnx_local.commit()

# Delete the CSV File after Data is inserted in the Database
try:
    os.remove(filename)
    print('CSV File deleted')
except:
    print("File Deletion Failed")
    
'''
def ObjectId(str):
    return str

def ISODate(str):
    return str


input_list =[ {
    "_id" : ObjectId("5ee394a1543a5b42af2d55c9"),
    "driver_id" : ObjectId("5ed25d1649faa2537e6ddd78"),
    "source" : "API",
    "url" : "https://easytaxidocs-me.s3-us-west-2.amazonaws.com/live/driver/selfies/5ed25d1649faa2537e6ddd78/5ee3949d543a5b42af2d55c8.jpeg",
    "status" : "unverified",
    "is_profile_photo" : False,
    "score" : 0,
    "created_at" : ISODate("2020-06-12T14:43:45.825Z"),
    "updated_at" : ISODate("2020-06-12T14:43:45.825Z")
    },

    {
    "_id" : ObjectId("5ee3a254543a5b6ba12a85a9"),
    "driver_id" : ObjectId("5ecf71a8bea298207b2ad189"),
    "source" : "API",
    "url" : "https://easytaxidocs-me.s3-us-west-2.amazonaws.com/live/driver/selfies/5ecf71a8bea298207b2ad189/5ee3a252543a5b6ba12a85a8.jpeg",
    "status" : "verified",
    "is_profile_photo" : False,
    "score" : 99.83,
    "created_at" : ISODate("2020-06-12T15:42:12.652Z"),
    "updated_at" : ISODate("2020-06-12T15:42:12.652Z")
    },

    {
    "_id" : ObjectId("5ee5cda17a88a727ed14c289"),
    "driver_id" : ObjectId("5ed20e8249faa2061675ee78"),
    "source" : "API",
    "url" : "https://easytaxidocs-me.s3-us-west-2.amazonaws.com/live/driver/selfies/5ed20e8249faa2061675ee78/5ee5cd9f7a88a727ed14c288.jpeg",
    "status" : "verified",
    "is_profile_photo" : False,
    "score" : 99.47,
    "created_at" : ISODate("2020-06-14T07:11:29.867Z"),
    "updated_at" : ISODate("2020-06-14T07:11:29.867Z")
    } ]

#print(input_list)


def drivers_face_verification_list_parser (input_list):
    _id,driver_id,source,url,status,is_profile_photo,score,created_at,updated_at=[],[],[],[],[],[],[],[],[]
    
    for ite in input_list:
        _id.append(ite['_id'])
        driver_id.append(ite['driver_id'])
        source.append(ite['source'])
        url.append(ite['url'])
        status.append(ite['status'])
        is_profile_photo.append(ite['is_profile_photo'])
        score.append(ite['score'])
        created_at.append(ite['created_at'])
        updated_at.append(ite['updated_at'])
        
    return pd.DataFrame({'_id' : _id,
                        'driver_id':driver_id,
                        'source':source,
                        'url':url,
                        'status':status,
                        'is_profile_photo':is_profile_photo,
                        'score':score,
                        'created_at':created_at,
                        'updated_at':updated_at})

'''