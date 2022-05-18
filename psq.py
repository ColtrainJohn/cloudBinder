ListTables = "SELECT relname FROM pg_class WHERE relkind='r' AND relname !~ '^(pg_|sql_)';"
DropTable = "DELETE FROM maintable;"
SelectAll = "SELECT * FROM maintable;"

### /TABLE CREATION\ ###
CreateTable = """
CREATE TABLE maintable (
    vgarusId VARCHAR (100),
    pickDate VARCHAR (100), 
    loadDate VARCHAR (100), 
    federal VARCHAR (100),
    region VARCHAR (100), 
    pango VARCHAR (100), 
    parus VARCHAR (100),
    seqArea VARCHAR (100)
    );
"""
CreateTable1 = """
CREATE TABLE table1 (
    vgarusId VARCHAR (100),
    pickDate VARCHAR (100), 
    loadDate VARCHAR (100), 
    federal VARCHAR (100),
    region VARCHAR (100), 
    pango VARCHAR (100), 
    parus VARCHAR (100),
    who VARCHAR (100),
    whoLine VARCHAR (100),
    year VARCHAR (100),
    month VARCHAR (100),
    week VARCHAR (100),
    monthYear VARCHAR (100)
    );
"""
CreateTable2 = """
CREATE TABLE table2 (
    year VARCHAR (100),
    region VARCHAR (100), 
    who VARCHAR (100),
    counts VARCHAR (100),
    percent VARCHAR (100),
    total VARCHAR (100)
);
"""
CreateTable3 = """
CREATE TABLE table3 (
    wholine VARCHAR (100),
    line VARCHAR (100), 
    ncount VARCHAR (100)
);
"""
makeTableByName = {
        "maintable" : CreateTable,
        "table1" : CreateTable1,
        "table2" : CreateTable2,
        "table3" : CreateTable3
    }
### \TABLE CREATION/ ###



pieChartLineageCounts = """
CREATE TABLE pieChartLineageCounts (
    who VARCHAR (100),
    wholine VARCHAR (100),
    ncount VARCHAR (100)
    );
"""
tableRegionWHO = """
CREATE TABLE tableRegionWHO (
    region VARCHAR (100),
    who VARCHAR (100),
    date VARCHAR (100),
    n VARCHAR (100)
    );
"""
cmd = """
SELECT
    column_name,
    data_type
FROM
    information_schema.columns
WHERE
    table_name = 'maintable';

"""


### /INSERTS\ ###
Insert = """
INSERT INTO 
    maintable (
        vgarusid,
        pickdate,
        loaddate,
        federal,
        region,
        pango,
        parus,
        seqArea
    ) 
VALUES %s"""
InsertTable1 = """
INSERT INTO 
    table1 (
        vgarusid,
        pickdate,
        loaddate,
        federal,
        region,
        pango,
        parus,
        who,
        whoLine,
        year,
        month,
        week,
        monthYear
    ) 
VALUES %s"""
InsertTable2 = """
INSERT INTO 
    table2 (
        year,
        region,
        who,
        counts,
        percent,
        total
    ) 
VALUES %s"""
InsertTable3 = """
INSERT INTO 
    table3 (
        wholine,
        line,
        ncount
    ) 
VALUES %s"""
inserTo = {
        "maintable" : Insert,
        "table1" : InsertTable1,
        "table2" : InsertTable2,
        "table3" : InsertTable3,
    }
### \INSERTS/ ###


DeleteDuplicates = """
DELETE   FROM maintable T1
  USING       maintable T2
WHERE  T1.ctid < T2.ctid       -- delete the "older" ones
  AND  T1.vgarusid = T2.vgarusid       -- list columns that define duplicates
"""
