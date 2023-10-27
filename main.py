import base64
from datetime import datetime
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Metric,
    RunReportRequest,
)
from google.oauth2 import service_account
import json
import os
import pandas as pd

#secrets 
pk_base64 = base64.b64decode(os.environ['PRIVATE_KEY'])
PRIVATE_KEY = pk_base64.decode("ascii")
CLIENT_EMAIL = os.environ['CLIENT_EMAIL']
TOKEN_URI = os.environ['TOKEN_URI']

json_acct_info = {
  "type": "service_account",
  "private_key": PRIVATE_KEY,
  "client_email": CLIENT_EMAIL,
  "token_uri": TOKEN_URI
}

credentials = service_account.Credentials.from_service_account_info(
    json_acct_info)

def ga4_response_to_df(response):
    dim_len = len(response.dimension_headers)
    metric_len = len(response.metric_headers)
    all_data = []
    for row in response.rows:
        row_data = {}
        for i in range(0, dim_len):
            row_data.update({response.dimension_headers[i].name: row.dimension_values[i].value})
        for i in range(0, metric_len):
            row_data.update({response.metric_headers[i].name: row.metric_values[i].value})
        all_data.append(row_data)
    df = pd.DataFrame(all_data)
    return df

def sample_run_report(property_id, today_date, scoped_credentials):
    """Runs a simple report on a Google Analytics 4 property."""
    client = BetaAnalyticsDataClient(credentials=scoped_credentials)

    request = RunReportRequest(
        property=f"properties/{property_id}",
        dimensions=[Dimension(name="pagePath")],
        metrics=[Metric(name="activeUsers")],
        date_ranges=[DateRange(start_date="7daysAgo", end_date="today")],
    )
    response = client.run_report(request)

    df = ga4_response_to_df(response)
    df.head(10).to_json("data/" + str(today_date) + "test1.json", orient='records')


if __name__ == "__main__":
    today = datetime.today().strftime('%Y-%m-%d')
    property_id = '300851350'
    scoped_credentials_script = credentials
    sample_run_report(property_id, today, scoped_credentials_script)
