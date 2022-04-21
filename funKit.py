import psycopg2, requests, re, config, psq, sys
import pandas as pd
from math import ceil
from parseMeta import parseMetaJson
import psycopg2.extras as extras


### CORE FUNCTIONS
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
        with conn:
            with conn.cursor() as cursor:
                cursor.execute(psq.SelectAll)
                print(cursor.rowcount)
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
    finally:
        conn.close()
### \CORE FUNCTIONS

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


def getBase():
    conn = psycopg2.connect(**config.DBparam)
    try:
        with conn:
            with conn.cursor() as cursor:
                cursor.execute(psq.SelectAll)
                return pd.DataFrame(cursor.fetchall())
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
    finally:
        conn.close()


def doTable1():
    tab = agrTab(getBase())
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
    tab = agrTab(getBase())[[7,8]].value_counts().reset_index()
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


def doTableRegion():
    tab = agrTab(getBase()).groupby([4, 7, 1]).agg({0 : 'count'})
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


def chooseWHOlineage(line):
    if line[5].find('Omicron') > -1 or line[6].find('Omicron') > -1:
        return 'Omicron (B.1.1.529 + BA.*)'
    elif line[5].find('Delta') > -1 or line[5].find('AY.') > -1 or line[6].find('Delta') > -1:
        return 'Delta (B.1.617.2 + AY.*)'
    elif line[5].find('Alpha') > -1 or line[5].find('Q.') > -1 or line[6].find('Alpha') > -1:
        return 'Alpha (B.1.1.7 + Q.*)'
    elif line[5].find('Beta') > -1 or line[6].find('Beta') > -1:
        return 'Beta (B.1.351)'
    elif line[5].find('Gamma') > -1 or line[6].find('Gamma') > -1:
        return 'Gamma (P.1)'
    return 'Не относится к "Variants of Concern"'


def makeMagic(line):
    if line[7].find('Omicron') > -1:
        if line[5].find('BA.') > -1:
            return line[5]
        else:
            return 'B.1.1.529'
    if line[7].find('Delta') > -1:
        if line[5].find('AY.') > -1:
            return line[5]
        else:
            return 'B.1.617.2'
    if line[7].find('Alpha') > -1:
        if line[5].find('Q.') > -1:
            return line[5]
        else:
            return 'B.1.1.7'
    if line[7].find('Beta') > -1:
            return 'B.1.351'
    if line[7].find('Gamma') > -1:
            return 'P.1'
    if line[7].find("Variants of Concern") > -1:
            return line[5]


def fixOmicron(x):
    if not x.startswith('B.'):
        x = '.'.join(x.split('.')[:2])
    return x


def agrTab(tab):
    tab[7] = tab.apply(lambda x: chooseWHOlineage(x), axis=1)
    tab[8] = tab.apply(lambda x: makeMagic(x), axis=1)
    tab[8] = tab[8].apply(lambda x: fixOmicron(x))
    return tab
