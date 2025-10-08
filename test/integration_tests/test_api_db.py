from airflow.exceptions import AirflowSkipException
from sqlalchemy import create_engine, inspect
import pytest
from src.fetch_data import fetch_data_api
from datetime import datetime
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO)

def test_api_db_behaviour(monkeypatch):
    # Create in-memory db
    db_engine = create_engine("sqlite:///:memory:")

    logical_date = datetime(2018,7,5) # Date with data

    # As sqlite don't support schemas, we need to catch the insertion in a schema and remove it
    original_to_sql = pd.DataFrame.to_sql

    def patched_to_sql(self, name, con, **kwargs):
        kwargs.pop("schema", None)
        return original_to_sql(self, name, con, **kwargs)

    monkeypatch.setattr(pd.DataFrame, "to_sql", patched_to_sql)

    try:
        fetch_data_api(db_engine, logical_date)

        tables = inspect(db_engine).get_table_names() # Get the table names

        assert 'stage_table' in tables # test if stage_table was created
        logging.info("✅ Look for stage_table in the db test passed successfully! : 1/2 passed")

        data = pd.read_sql("SELECT * FROM stage_table", db_engine) # Read the data from stage_table

        assert not data.empty # Test if the data is not empty
        logging.info("✅ Non empty data in the db test passed successfully! : 2/2 passed")

    except AirflowSkipException:
        pytest.skip("No data available for the current date")