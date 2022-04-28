ListTables = "SELECT relname FROM pg_class WHERE relkind='r' AND relname !~ '^(pg_|sql_)';"
DropTable = "DELETE FROM maintable;"
SelectAll = "SELECT * FROM maintable;"
CreateTable = """
CREATE TABLE maintable (
    vgarusId VARCHAR (100),
    pickDate VARCHAR (100), 
    loadDate VARCHAR (100), 
    federal VARCHAR (100),
    region VARCHAR (100), 
    pango VARCHAR (100), 
    parus VARCHAR (100)
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
    week VARCHAR (100)
    );
"""
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
Insert = """
INSERT INTO 
    maintable (
        vgarusid,
        pickdate,
        loaddate,
        federal,
        region,
        pango,
        parus
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
        week
    ) 
VALUES %s"""
InsertTable2 = """
INSERT INTO 
    pieChartLineageCounts (
        who,
        wholine,
        ncount
    ) 
VALUES %s"""
InsertTableRegionWHO = """
INSERT INTO 
    tableRegionWHO (
        region,
        who,
        date,
        n
        )
VALUES %s"""

DeleteDuplicates = """
DELETE   FROM maintable T1
  USING       maintable T2
WHERE  T1.ctid < T2.ctid       -- delete the "older" ones
  AND  T1.vgarusid = T2.vgarusid       -- list columns that define duplicates
"""
