import requests, psycopg2, config, psq, magicFun, tqdm
import pandas as pd
from math import ceil # ?
from tqdm import tqdm
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
    translation = pd.DataFrame([magicFun.parseMetaJson(line) for line in rawJson]).astype('str')
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


def workAct(pageAmount, volume, fun):
    date = str(pd.to_datetime('today'))
    try:
        restartBase('maintable')
        with doSesh() as sesh:
            for page in tqdm(range(1, pageAmount + 1)):
                fun(sesh, page)
        updateTables()
        with open('log_schedule', 'a') as log:
            log.write(date + '\t' + str(volume) + '\n')
    except Exception as error:
        with open('log_schedule', 'a') as log:
            log.write(date + '\t' + str(error) + '\n')  



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

        
def updateTable(basename):
    data = magicFun.shapeTab(getBase(), basename)
    pageTuples = [tuple(x) for x in data.to_numpy()]
    baseExecute(f'DELETE FROM {basename};')
    putToDatabase(pageTuples, basename)


def updateTables(tables=['table1', 'table2', 'table3']):
    for each in tables:
        updateTable(each)
        print(each)

def dateUpdate():
    restartBase('updatedate')
    command = psq.InsertUpdateDate % "('" + str(pd.to_datetime('now')).split()[0] + "')"
    #command = psq.InsertUpdateDate % '(' + '10.02.2022' + ')'
    print(command)
    baseExecute(command)
            
            

### \DATABASE MANUPULATIONS/ ###
