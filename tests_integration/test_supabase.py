import unittest
import json
import psycopg2
from law_reader import SupabaseDBInterface


# Helper method that takes the json file to draw the data from, and the table to insert it into, and inserts the data
# into the table
def insert_data_from_json_file(json_file: str, table_name: str, db_interface: SupabaseDBInterface):
    with open(json_file, "r") as f:
        data = json.load(f)
        db_interface.supabase_connection.table(table_name).insert(data).execute()

class TestDatabase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        db_interface = SupabaseDBInterface(debug=True)

        # Tuples of table to delete and column to use in where statement when deleting
        tables_and_columns_to_delete = [
            ("Summaries", "summary_id"),
            ("Revisions", "revision_guid"),
            ("Revision_Text", "rt_unique_id"),
            ("Bills", "bill_number")
        ]
        # Delete all entries in the Bills, Summaries, Revisions, and Revision_Text tables
        for table, column in tables_and_columns_to_delete:
            db_interface.supabase_connection.table(table).delete().neq(column, "-1").execute()

        cls.db_interface = db_interface

        # Variable containing tuples of json file and table name to insert into
        json_files_and_tables = [
            ("test_data/bills_data.json", "Bills"),
            ("test_data/revisions_text_data.json", "Revision_Text"),
            ("test_data/revisions_data.json", "Revisions"),
        ]

        # Load data from json files and insert into tables
        for json_file, table_name in json_files_and_tables:
            insert_data_from_json_file(json_file, table_name, db_interface)

        # Load data from "bills_data.json" and insert into bills table
        insert_data_from_json_file("tests_integration/test_data/bills_data.json", "Bills", db_interface)


    def setUp(self):
        pass

    def test_database(self):
        pass