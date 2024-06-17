from psycopg import sql
import csv
from common import MetaData, Connection


metadata = MetaData()
tables = metadata.tables
connection = Connection()
conn = connection.conn


def compute_ndv(cur, column_name, table_name):
    cur.execute(
        sql.SQL(
            """
        SELECT 
            COUNT(DISTINCT({col})) / NULLIF(cast(COUNT({col}) as decimal),0), 
            COUNT(DISTINCT({col})), 
            COUNT({col})
        FROM {tbl}
        """
        ).format(
            col=sql.Identifier(column_name),
            tbl=sql.Identifier(table_name),
        )
    )


with open("ndv.csv", "w") as f:
    writer = csv.writer(f)
    writer.writerow(
        ["table_name", "data_type", "column_name", "ndv", "count_distinct", "count"]
    )
    with conn.cursor() as cur:
        for tbl in tables.keys():
            for _type in tables[tbl].keys():
                for c in tables[tbl][_type]:
                    compute_ndv(cur, c, tbl)
                    for ndv, count_distinct, count in cur:
                        writer.writerow([tbl, _type, c, ndv, count_distinct, count])
                        print(tbl, _type, c, ndv, count_distinct, count)
