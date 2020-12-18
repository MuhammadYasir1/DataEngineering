# -*- coding: utf-8 -*-
"""
Created on Fri Dec 18 12:58:37 2020

@author: dell
"""
#from datetime import date,timedelta,datetime
import datetime
import pyodbc

def returnDate(a):
    currentdatetime = datetime.datetime.utcnow()
    tmp = currentdatetime + datetime.timedelta(days=a)
    return datetime.datetime(tmp.year, tmp.month, tmp.day, 0, 0, 0, 0)

def getMEDB():
    return pyodbc.connect(r'DRIVER={SQL Server Native Client 11.0};SERVER=WIN-6JH4R0TL0JL\NEWETBI;DATABASE=etbi;UID=WIN-6JH4R0TL0JL\administrator;PWD=Imemewt2016;Trusted_Connection=yes')

def setLogger(fileName):
    import logging
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    fh = logging.FileHandler(fileName)
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    
def formatObjectID(objectID):
    if objectID is None:
        return None
    else:
        return objectID
