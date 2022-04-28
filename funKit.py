import sys, requests, re, psycopg2, config, psq, magicFun
import pandas as pd
from math import ceil # ?
from parseMeta import parseMetaJson
import psycopg2.extras as extras



### /CORE FUNCTIONS\ ###
def measureVolume(*args, **kwargs):
    volume = int(requests.get(config.urlSeqCount, headers=config.headers).text)
    pageAmount = ceil(volume / config.pageSize)
    return volume, pageAmount


def doSesh():
    sesh = requests.Session()
    sesh.headers.update(config.headers)
    return sesh


def takeCloudJson(sesh, pageNum):
    response = sesh.get(config.urlSeqMeta, params={"page_id" : pageNum, "page_size" : 1000})
    rawJson = response.json()
    return rawJson


def translate(rawJson):
    translation = pd.DataFrame([parseMetaJson(line) for line in rawJson]).astype('str')
    pageTuples = [tuple(x) for x in translation.to_numpy()]
    return pageTuples


def putToDatabase(pageTuples):
    conn = psycopg2.connect(**config.DBparam)
    try:
        with conn:
            with conn.cursor() as cursor:
                extras.execute_values(cursor, psq.Insert, pageTuples, page_size=1000)
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
    finally:
        conn.close()
### \CORE FUNCTIONS/ ###


### /DATABASE MANUPULATIONS\ ###
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
                cursor.execute("SELECT * FROM maintable;")
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


def getBase(basename='maintable'):
    conn = psycopg2.connect(**config.DBparam)
    try:
        with conn:
            with conn.cursor() as cursor:
                cursor.execute(f'SELECT * FROM {basename};')
                return pd.DataFrame(cursor.fetchall())
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
    finally:
        conn.close()


def doTable1():
    tab = magicFun.agrTab(getBase())
    tab = tab.loc[(tab[1] != 'None') & (tab[4].isin(config.regions))]
    pageTuples = [tuple(x) for x in tab.to_numpy()]
    conn = psycopg2.connect(**config.DBparam)
    try:
        with conn:
            with conn.cursor() as cursor:
                cursor.execute("DELETE FROM table1;")
        with conn:
            with conn.cursor() as cursor:
                extras.execute_values(cursor, psq.InsertTable1, pageTuples, page_size=1000)
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
    finally:
        conn.close()


def doTable2():
    tab = magicFun.agrTab(getBase())[[7,8]].value_counts().reset_index()
    pageTuples = [tuple(x) for x in tab.to_numpy()]
    conn = psycopg2.connect(**config.DBparam)
    try:
        with conn:
            with conn.cursor() as cursor:
                cursor.execute("DELETE FROM table2;")
        with conn:
            with conn.cursor() as cursor:
                extras.execute_values(cursor, psq.InsertTable2, pageTuples, page_size=1000)
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
    finally:
        conn.close()


# def getWeeklyTab(data):
#     regionCounts = tab[3].value_counts(


def doTableRegion():
    tab = magicFun.agrTab(getBase()).groupby([4, 7, 1]).agg({0 : 'count'})
    tab = tab.reindex(pd.MultiIndex.from_product([config.regions, tab.index.levels[1], tab.index.levels[2]]), fill_value=0).reset_index()
    pageTuples = [tuple(x) for x in tab.to_numpy()]
    conn = psycopg2.connect(**config.DBparam)
    try:
        with conn:
            with conn.cursor() as cursor:
                cursor.execute("DELETE FROM tableRegionWHO;")
        with conn:
            with conn.cursor() as cursor:
                extras.execute_values(cursor, psq.InsertTableRegionWHO, pageTuples, page_size=1000)
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
    finally:
        conn.close()


def updateOthers():
    doTable1()
    doTable2()
    doTableRegion()
### \DATABASE MANUPULATIONS/ ###
