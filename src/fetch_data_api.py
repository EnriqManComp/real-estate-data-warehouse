import pandas as pd
import requests
from io import StringIO

base_url = "https://data.ct.gov/resource/5mzw-sjtu.csv"
params = {
    'daterecorded': '2018-07-5T00:00:00.000',
    '$limit': 1000,
    '$offset': 0
}

daily_data = pd.DataFrame()

while True:

    results = requests.get(base_url, params=params)
    # Convert to pandas DataFrame
    results_df = pd.read_csv(StringIO(results.text))

    if results_df.empty:
        break

    daily_data = pd.concat([daily_data, results_df], axis=0)

    params['$offset'] += params['$limit']


