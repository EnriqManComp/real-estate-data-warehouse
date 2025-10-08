import pandas as pd
import requests
from io import StringIO
from airflow.exceptions import AirflowSkipException
from datetime import datetime

def fetch_data_api(db_engine, current_date):
    """ Fetch the data from the gov API, limit set to 1000 records """

    # Formatting the logical day
    if isinstance(current_date, str): # Catch if is a string to avoid error on airflow due to Jinja template
        current_date = datetime.strptime(current_date, '%Y-%m-%d')

    day = current_date.strftime("%Y-%m-%dT00:00:00.000")

    # base api url
    base_url = "https://data.ct.gov/resource/5mzw-sjtu.csv"

    # Normalize field names
    field_names = [
        "serial_number",
	    "list_year",
	    "date_recorded",
	    "town",
	    "address",
	    "assessed_value",
	    "sale_amount",
	    "sales_ratio",
	    "property_type",
	    "residencial_type",
	    "non_use_code",
	    "assessor_remarks",
	    "opm_remarks",
	    "location"
    ]

    # Headers for URL
    params = {
        'daterecorded': day,
        '$limit': 1000,
        '$offset': 0
    }

    # Creating dataframe for daily data
    daily_data = pd.DataFrame() 

    while True:
        # Request data from api
        results = requests.get(base_url, params=params)
        try:
            # Convert to pandas DataFrame
            results_df = pd.read_csv(StringIO(results.text))
        except pd.errors.EmptyDataError:
            break
        # Break the loop in case not data
        if results_df.empty:                # [:test-tag: units_and_components.Empty data test] ✅
            break   
        # Concatenate data if limit exceed 1000 records
        daily_data = pd.concat([daily_data, results_df], axis=0)
        #daily_data = daily_data.reset_index(drop=True)
        # Update offset 
        params['$offset'] += params['$limit']   # [:test-tag: units_and_components.Non-empty data test] ✅

    # Push raw data into Postgres staging table
    if not daily_data.empty:
        # Rename field names
        daily_data.columns=field_names
        # Push to postgres
        daily_data.to_sql("stage_table", db_engine, if_exists='replace', index=False, schema="high_roles") # [:test-tag: units_and_components.Hit db test] ✅ Tested
    else:
        # Skip if empty data for the logical date
        raise AirflowSkipException("No data found, skipping downstream tasks.") # [:test-tag: units_and_components.Empty data test] ✅ Tested
    

