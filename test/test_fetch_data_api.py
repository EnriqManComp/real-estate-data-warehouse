import pytest
import pandas as pd
import logging
from airflow.exceptions import AirflowSkipException
from unittest.mock import patch, Mock

logging.basicConfig(level=logging.INFO)


# [:test-tag: Empty data] ✅
def test_fetch_data_empty(mocker): 
    """ Test if fetch_data_api raises an AirflowSkipException when an empty data is returned from api call """
    db_engine = object() # dummy engine

    mock_response = mocker.Mock()
    mock_response.text = ""  # empty string simulates no CSV data
    mock_requests = mocker.patch("src.fetch_data.requests.get", return_value=mock_response)
    
    # Mock pandas.read_csv to return empty DataFrame
    mock_read_csv = mocker.patch("src.fetch_data.pd.read_csv")
    mock_read_csv.return_value = pd.DataFrame()

    from src.fetch_data import fetch_data_api

    logical_date = pd.Timestamp("2024-01-01")

    with pytest.raises(AirflowSkipException):
        fetch_data_api(db_engine, logical_date)

    logging.info("✅ Empty data test passed successfully! : 1/3 passed")

# [:test-tag: Non-empty data] ✅
def test_non_empty_data(mocker):
    """ Test non empty data get from the api request """
    db_engine = object() # mock a dummmy engine

    mock_response = mocker.Mock()
    mock_response.text = "serial_number,list_year\n1,2024"  # CSV string

    mock_response_2 = mocker.Mock()
    mock_response_2.text = "serial_number,list_year\n2,2024" # CSV string

    mock_response_3 = mocker.Mock()
    mock_response_3.text = "serial_number,list_year\n" # Empty string
   
    mock_requests =  mocker.patch("src.fetch_data.requests.get", side_effect=[mock_response, mock_response_2, mock_response_3])# mock requests from api
    
    mock_to_sql = mocker.patch("src.fetch_data.pd.DataFrame.to_sql") # mock the sql insertion

    logical_date = pd.Timestamp("2024-01-01")

    from src.fetch_data import fetch_data_api

    # ---------- Safe column patch ----------
    # Create a subclass of DataFrame that ignores column assignment
    class DummyDataFrame(pd.DataFrame):
        @property
        def columns(self):
            return super().columns

        @columns.setter
        def columns(self, value):
            # Ignore assignment to prevent LengthMismatch
            pass

    # Patch pd.concat to return DummyDataFrame so daily_data is safe
    original_concat = pd.concat
    # objs is the list of df that 
    mocker.patch("pandas.concat", side_effect=lambda objs, axis=0: DummyDataFrame(objs[0])) 

    try:
        fetch_data_api(db_engine, logical_date)
    except AirflowSkipException:
        pass

    assert mock_requests.call_count == 3, "Requests calls were skip it"
    logging.info("✅ Pagination test passed successfully! : 2/3 passed")
    
# [:test-tag: units_and_components.Hit db test] ✅ Tested
@patch("src.fetch_data.requests.get")
@patch("src.fetch_data.pd.DataFrame.to_sql")
def test_hit_db(mock_to_sql, mock_get):

    # Mock data to simulate API response
    MOCK_CSV = """serial_number,list_year,date_recorded,town,address,assessed_value,sale_amount,sales_ratio,property_type,residencial_type,non_use_code,assessor_remarks,opm_remarks,location
    1,2025,2025-10-07,Town1,Address1,100000,120000,1.2,Residential,Single,NA,Remark1,Remark2,"POINT(-72.123 41.123)"
    2,2025,2025-10-07,Town2,Address2,200000,220000,1.1,Residential,Multi,NA,Remark3,Remark4,"POINT(-72.456 41.456)"
    """

    EMPTY_CSV = ""  # used to stop the loop

    mock_get.side_effect = [
        Mock(text=MOCK_CSV),
        Mock(text=EMPTY_CSV)
    ]

    # Mock the engine
    mock_engine = Mock()

    logical_date = pd.Timestamp("2024-01-01")
    
    from src.fetch_data import fetch_data_api
    fetch_data_api(mock_engine, logical_date)
    # Assert to_sql was called exactly once
    assert mock_to_sql.called
    # Assert it was called with the correct table name
    mock_to_sql.assert_called_with("stage_table", mock_engine, if_exists='replace', index=False, schema="high_roles")
    logging.info("✅ Push data to database test passed successfully! : 3/3 passed")

