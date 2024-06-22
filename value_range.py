from psycopg import sql
import csv
from common import MetaData, Connection


metadata = MetaData()
tables = metadata.tables
connection = Connection()
conn = connection.conn


def compute_minmax_bytelength(cur, column_name, table_name):
    """
    For string columns
    """
    cur.execute(
        sql.SQL(
            """
            SELECT MIN(octet_length({col})), MAX(octet_length({col}))
            FROM {tbl}
            """
        ).format(col=sql.Identifier(column_name), tbl=sql.Identifier(table_name))
    )


def compute_minmax_numeric(cur, column_name, table_name):
    """
    For int or float columns
    """
    cur.execute(
        sql.SQL(
            """
                SELECT MIN(ABS({col})), MAX(ABS({col}))
                FROM {tbl}
                """
        ).format(col=sql.Identifier(column_name), tbl=sql.Identifier(table_name))
    )


with open("value_range.csv", "w") as f:
    writer = csv.writer(f)
    writer.writerow(
        [
            "table_name",
            "data_type",
            "column_name",
            "min",
            "max",
        ]
    )

    with conn.cursor() as cur:
        for tbl in tables.keys():
            for _type in tables[tbl].keys():
                for c in tables[tbl][_type]:
                    if _type == "string":
                        compute_minmax_bytelength(cur, c, tbl)
                    elif _type != "timestamp":
                        compute_minmax_numeric(cur, c, tbl)
                    for min_val, max_val in cur:
                        row_to_write = [tbl, _type, c, min_val, max_val]
                        writer.writerow(row_to_write)
                        print(row_to_write)
