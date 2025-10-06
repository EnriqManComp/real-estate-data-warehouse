import pytest
import pandas as pd

from airflow.exceptions import AirflowSkipException

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

# [:test-tag: Non-empty data] ❌
def test_non_empty_data(mocker):
    """ Test non empty data get from the api request """
    db_engine = object() # mock a dummmy engine

    mock_response = mocker.Mock()
    mock_response.text = "serial_number,list_year\n1,2024"  # CSV string
   
    mock_requests =  mocker.patch("src.fetch_data.requests.get", return_value=mock_response)# mock requests from api
    mock_read_csv = mocker.patch("src.fetch_data.pd.read_csv") # mock pandas read csv
    mock_to_sql = mocker.patch("src.fetch_data.pd.DataFrame.to_sql") # mock the sql insertion

    # Fake data
    fake_real_estate_data = pd.DataFrame({
        'serial_number': [1],
        'list_year': [2024]
    })
    mock_read_csv.side_effect = [fake_real_estate_data, pd.DataFrame()]

    logical_date = pd.Timestamp("2024-01-01")

    from src.fetch_data import fetch_data_api
    fetch_data_api(db_engine, logical_date)

    assert mock_requests.called
    assert mock_read_csv.call_count == 2
    assert mock_to_sql.called
    


