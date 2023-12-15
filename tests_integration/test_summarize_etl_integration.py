import unittest
import json
from unittest.mock import patch

from db_interfaces.PostgresDBInterface import PostgresDBInterface
from law_reader import DBInterface
from law_reader.summarize_etl import summarize_all_unsummarized_revisions


def load_json_test_data(json_file: str, table: str, db_interface: DBInterface) -> str:
    """
    Helper method that loads the json test data from the given file
     and inserts it into the given table
    :param table: the table to insert the data into
    :param json_file: the file to load the json data from
    :param db_interface: the DBInterface to use to insert the data
    """
    # Read JSON file as dictionary
    with open(json_file, "r") as f:
        data: list[dict] = json.load(f)
        # Insert data into the table
        for entry in data:
            db_interface.insert(table, entry)


# Stub function to simulate the summarize function
def summarize_stub(text: str) -> str:
    return f"Summary: {text}"


class TestSummarizeETLIntegration(unittest.TestCase):

    def setUp(self):
        # Connect to Database Dev
        self.db_interface = PostgresDBInterface(
            db_password="postgres",
            db_host="localhost")

        # Populate the database with the test data
        load_json_test_data("test_data/bills_data.json", "Bills", self.db_interface)
        load_json_test_data("test_data/revisions_data.json", "Revisions", self.db_interface)
        load_json_test_data("test_data/revisions_text_data.json", "Revision_Text", self.db_interface)

        # Stub "summarize_etl"'s "summarize_bill" function

    # Patch the summarize_bill function with the stub
    @patch("law_reader.summarize_etl.summarize_bill", new=summarize_stub)
    def test_summarize_etl_integration(self):
        summarize_all_unsummarized_revisions(self.db_interface)

        # Check that the summaries were inserted into the database correctly
        self.db_interface.execute("SELECT * FROM \"Summaries\"")
        summaries = self.db_interface.fetchall()
        self.assertEqual(len(summaries), 6)
