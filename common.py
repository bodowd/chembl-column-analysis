import psycopg
import csv

INTS = ["bigint", "smallint", "integer"]
FLOATS = ["numeric"]
TS = ["timestamp without time zone"]
STRINGS = ["text", "character varying"]


class Connection:
    def __init__(self):
        self.conn = psycopg.connect(
            "postgresql://postgres:example@localhost:5435/chembl_33"
        )

    def __exit__(self):
        self.conn.close()


class MetaData:
    def __init__(self):
        self.data_types = []
        self.tables = {}
        self.get_data_types()
        self.get_column_metadata()

    def get_data_types(self):
        with open("data_types.csv", "r") as f:
            reader = csv.reader(f)
            # skip header
            next(reader)
            for (data_type,) in reader:
                self.data_types.append(data_type)

    def get_column_metadata(self):
        with open("column_metadata.csv", "r") as f:
            reader = csv.reader(f)
            # skip header
            next(reader)

            for table_name, column_name, data_type in reader:
                if table_name not in self.tables:
                    self.tables[table_name] = {}
                    self.tables[table_name]["int"] = []
                    self.tables[table_name]["float"] = []
                    self.tables[table_name]["string"] = []
                    self.tables[table_name]["timestamp"] = []

                if data_type in INTS:
                    self.tables[table_name]["int"].append(column_name)
                elif data_type in FLOATS:
                    self.tables[table_name]["float"].append(column_name)
                elif data_type in TS:
                    self.tables[table_name]["timestamp"].append(column_name)
                elif data_type in STRINGS:
                    self.tables[table_name]["string"].append(column_name)

            # quick check
            assert (
                len(self.tables["assays"]["int"]) == 9
            ), "should be 9 columns with integers in 'assays' table"
            assert (
                len(self.tables["assays"]["string"]) == 15
            ), "should be 15 columns with character varying in 'assays' table"
