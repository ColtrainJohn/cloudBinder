import psycopg2, requests, re, config, psq, sys
import pandas as pd
from math import ceil
import psycopg2.extras as extras

def measureVolume(*args, **kwargs):
    volume = int(requests.get(config.urlSeqCount, headers=config.headers).text)
    pageAmount = ceil(volume / config.pageSize)
    return volume, pageAmount


def doSesh():
    sesh = requests.Session()
    sesh.headers.update(config.headers)
    return sesh


def takeFromCloud(sesh, pageNum):
    page = sesh.get(
        url=config.urlSeqMeta,
        params={
            "page_id" : pageNum,
            "page_size" : 1000
                }
            )
    return page


def translate(rawJson):
    translation = pd.concat(
        [
            pd.json_normalize(rawJson)[config.columnSourceNames], 
            pd.json_normalize(rawJson, record_path='addresses')[config.geoLoc]
        ], axis=1
    )[config.colToCol.keys()].rename(columns=(config.colToCol)).astype('str')
    translation = translation.where(pd.notnull(translation), None)
    pageTuples = [tuple(x) for x in translation.to_numpy()]
    return pageTuples[:1000]


def putToDatabase(pageTuples):
    conn = psycopg2.connect(**config.DBparam)
    try:
        with conn:
            with conn.cursor() as cursor:
                extras.execute_values(cursor, psq.Insert, pageTuples, page_size=1000)
        with conn:
            with conn.cursor() as cursor:
                cursor.execute(psq.SelectAll)
                print(cursor.rowcount)
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
    finally:

        conn.close()


def oneByOnePutToBase(pageTuples):
    conn = psycopg2.connect(**config.DBparam)
    for each in pageTuples:
        try:
            with conn:
                with conn.cursor() as cursor:
                    extras.execute_values(cursor, psq.Insert, pageTuples, page_size=10)
                    print(each)
            with conn:  
                with conn.cursor() as cursor:
                    cursor.execute(psq.SelectAll)
                    print(cursor.rowcount)
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error: %s" % error)
        finally:
            conn.close()



def baseExecute(cmd):
    conn = psycopg2.connect(**config.DBparam)
    try:
        with conn:
            with conn.cursor() as cursor:
                cursor.execute(cmd)
                return True
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        return False
    finally:
        conn.close()


def checkBaseContant():
    conn = psycopg2.connect(**config.DBparam)
    try:
        with conn:
            with conn.cursor() as cursor:
                print(cursor.rowcount)               
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
    finally:
        conn.close()

def reloadDatabase():
    conn = psycopg2.connect(**config.DBparam)
    try:
        with conn:
            with conn.cursor() as cursor:
                cursor.execute("DROP TABLE maintable;")
        with conn:
            with conn.cursor() as cursor:
                cursor.execute(psq.CreateTable)
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
    finally:
        conn.close()


def getBase():
    conn = psycopg2.connect(**config.DBparam)
    try:
        with conn:
            with conn.cursor() as cursor:
                cursor.execute(psq.SelectAll)
                return cursor.fetchall()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        return False
    finally:
        conn.close()


