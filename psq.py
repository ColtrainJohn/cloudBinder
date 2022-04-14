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
