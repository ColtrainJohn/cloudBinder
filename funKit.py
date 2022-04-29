import requests, psycopg2, config, psq, magicFun
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


def putToDatabase(pageTuples, basename='maintable'):
    conn = psycopg2.connect(**config.DBparam)
    try:
        with conn:
            with conn.cursor() as cursor:
                extras.execute_values(cursor, psq.inserTo[basename], pageTuples, page_size=1000)
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
    finally:
        conn.close()
### \CORE FUNCTIONS/ ###



### /DATABASE MANUPULATIONS\ ###
def baseExecute(cmd, fetch=False):
    conn = psycopg2.connect(**config.DBparam)
    try:
        with conn:
            with conn.cursor() as cursor:
                cursor.execute(cmd)
                if fetch:
                    return pd.DataFrame(cursor.fetchall())
                return True
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        return False
    finally:
        conn.close()
  

def restartBase(basename='maintable'):
    baseExecute(f'DROP TABLE {basename};')
    baseExecute(psq.makeTableByName[basename])


def getBase(basename='maintable'):
    return pd.DataFrame(baseExecute(f'SELECT * FROM {basename};', fetch=True))
### \DATABASE MANUPULATIONS/ ###



### /TABLE CONNECTIONS\ ###
# def doTable1():
#     tab = magicFun.agrTab(getBase())
#     pageTuples = [tuple(x) for x in tab.to_numpy()]
#     conn = psycopg2.connect(**config.DBparam)
#     try:
#         with conn:
#             with conn.cursor() as cursor:
#                 cursor.execute("DELETE FROM table1;")
#         with conn:
#             with conn.cursor() as cursor:
#                 extras.execute_values(cursor, psq.InsertTable1, pageTuples, page_size=1000)
#     except (Exception, psycopg2.DatabaseError) as error:
#         print("Error: %s" % error)
#     finally:
#         conn.close()


# def doTable2():
#     tab = magicFun.variantRegionsTable(magicFun.agrTab(getBase()))
#     pageTuples = [tuple(x) for x in tab.to_numpy()]
#     conn = psycopg2.connect(**config.DBparam)
#     try:
#         with conn:
#             with conn.cursor() as cursor:
#                 cursor.execute("DELETE FROM table2;")
#         with conn:
#             with conn.cursor() as cursor:
#                 extras.execute_values(cursor, psq.InsertTable2, pageTuples, page_size=1000)
#     except (Exception, psycopg2.DatabaseError) as error:
#         print("Error: %s" % error)
#     finally:
#         conn.close()

        
# def doTable3():
#     data = magicFun.agrTab(getBase())[[7, 8]].value_counts().reset_index()
#     pageTuples = [tuple(x) for x in data.to_numpy()]
#     conn = psycopg2.connect(**config.DBparam)
#     try:
#         with conn:
#             with conn.cursor() as cursor:
#                 cursor.execute("DELETE FROM table3;")
#         with conn:
#             with conn.cursor() as cursor:
#                 extras.execute_values(cursor, psq.InsertTable3, pageTuples, page_size=1000)
#     except (Exception, psycopg2.DatabaseError) as error:
#         print("Error: %s" % error)
#     finally:
#         conn.close()
        
        
def updateTable(basename):
    data = magicFun.shapeTab(getBase(), basename)
    pageTuples = [tuple(x) for x in data.to_numpy()]
    baseExecute(f'DELETE FROM {basename};')
    putToDatabase(pageTuples, basename)
### \TABLE CONNECTIONS/ ###


def updateTables(tables=['table1', 'table2', 'table3']):
    for each in tables:
        updateTable(each)
        print(each)

