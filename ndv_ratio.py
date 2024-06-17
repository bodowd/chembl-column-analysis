from psycopg import sql
import psycopg
import csv

ints = ["bigint", "smallint", "integer"]
floats = ["numeric"]
ts = ["timestamp without time zone"]
strings = ["text", "character varying"]

data_types = []
with open("data_types.csv", "r") as f:
    reader = csv.reader(f)
    # skip header
    next(reader)
    for (data_type,) in reader:
        data_types.append(data_type)

with open("column_metadata.csv", "r") as f:
    reader = csv.reader(f)
    # skip header
    next(reader)

    tables = {}
    for table_name, column_name, data_type in reader:
        if table_name not in tables:
            tables[table_name] = {}
            tables[table_name]["int"] = []
            tables[table_name]["float"] = []
            tables[table_name]["string"] = []
            tables[table_name]["timestamp"] = []

        if data_type in ints:
            tables[table_name]["int"].append(column_name)
        elif data_type in floats:
            tables[table_name]["float"].append(column_name)
        elif data_type in ts:
            tables[table_name]["timestamp"].append(column_name)
        elif data_type in strings:
            tables[table_name]["string"].append(column_name)

    # quick check
    assert (
        len(tables["assays"]["int"]) == 9
    ), "should be 9 columns with integers in 'assays' table"
    assert (
        len(tables["assays"]["string"]) == 15
    ), "should be 15 columns with character varying in 'assays' table"

with psycopg.connect("postgresql://postgres:example@localhost:5435/chembl_33") as conn:
    with conn.cursor() as cur:
        for tbl in tables.keys():
            for _type in tables[tbl].keys():
                for c in tables[tbl][_type]:
                    cur.execute(
                        sql.SQL(
                            """
                        SELECT 
                            COUNT(DISTINCT({})) / NULLIF(cast(COUNT({}) as decimal),0), 
                            COUNT(DISTINCT({})), 
                            COUNT({})
                        FROM {}
                        """
                        ).format(
                            sql.Identifier(c),
                            sql.Identifier(c),
                            sql.Identifier(c),
                            sql.Identifier(c),
                            sql.Identifier(tbl),
                        )
                    )

                    for ndv, count_distinct, count in cur:
                        print(tbl, _type, c, ndv, count_distinct, count)
