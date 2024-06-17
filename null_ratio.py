from psycopg import sql
import csv
from common import MetaData, Connection


metadata = MetaData()
tables = metadata.tables
connection = Connection()
conn = connection.conn


def compute_null(cur, column_name, table_name):
    null_count = cur.execute(
        sql.SQL(
            """
            SELECT COUNT(*)
            FROM {tbl}
            WHERE {col} IS NULL
            """
        ).format(col=sql.Identifier(column_name), tbl=sql.Identifier(table_name))
    )
    return null_count


def compute_row_count(cur, column_name, table_name):
    row_count = cur.execute(
        sql.SQL(
            """
            SELECT COUNT(*)
            FROM {tbl}
            """
        ).format(col=sql.Identifier(column_name), tbl=sql.Identifier(table_name))
    )
    return row_count


with open("null_ratio.csv", "w") as f:
    writer = csv.writer(f)
    writer.writerow(
        [
            "table_name",
            "data_type",
            "column_name",
            "null_count",
            "row_count",
            "null_ratio",
        ]
    )

    with conn.cursor() as cur:
        for tbl in tables.keys():
            for _type in tables[tbl].keys():
                for c in tables[tbl][_type]:
                    null_count = 0
                    row_count = 0
                    null_count_cur = compute_null(cur, c, tbl)
                    assert len(null_count_cur._results) == 1
                    for (nc,) in null_count_cur:
                        null_count = nc

                    row_count_cur = compute_row_count(cur, c, tbl)
                    assert len(row_count_cur._results) == 1
                    for (rc,) in row_count_cur:
                        row_count = rc

                    row_to_write = [
                        tbl,
                        _type,
                        c,
                        null_count,
                        row_count,
                        null_count / row_count,
                    ]
                    writer.writerow(row_to_write)
                    print(row_to_write)
