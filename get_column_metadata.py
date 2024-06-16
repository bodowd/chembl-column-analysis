import psycopg
import csv


with psycopg.connect("postgresql://postgres:example@localhost:5435/chembl_33") as conn:
    tablenames = conn.execute(
        """
        SELECT tablename FROM pg_catalog.pg_tables
        WHERE schemaname='public' ;
        """
    )
    num_tables = 0
    data_types = set()
    with open("column_metadata.csv", "w") as f:
        writer = csv.writer(f)
        writer.writerow(["table_name", "column_name", "data_type"])

        for (tablename,) in tablenames:
            num_tables += 1

            result = conn.execute(
                """ 
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema='public'
                AND table_name=%s
                """,
                (tablename,),
            )

            for (
                column_name,
                data_type,
            ) in result:
                if data_type not in data_types:
                    data_types.add(data_type)
                writer.writerow([tablename, column_name, data_type])

    assert num_tables == 79, "should be 79 tables in chembl_33"
    with open("data_types.csv", "w") as f:
        writer = csv.writer(f)
        writer.writerow(["data_type"])
        for i in data_types:
            writer.writerow([i])

    print("data types: ", data_types)
