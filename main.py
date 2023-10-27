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
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "service_account.json"
import pandas as pd

#jsonfile = open("service_account.json") 
#json_acct_info = json.load(jsonfile)

cred_dict = {
  "type": "service_account",
  "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEugIBADANBgkqhkiG9w0BAQEFAASCBKQwggSgAgEAAoIBAQCzEFazQ6dOrDWi\nwLVk6pabsEWJ2zK+idVIdHWReBHTakCuo+n+jaAyqA0rKleDRK01WjV0GVHJG1x1\naKbD6kuMOj765ubclZ2lMZlayqilauZfbCAjw+NC0gVwc0M3Q0qIMs1NyB+nM8t/\nnqA/qIlvMjN0NOBYCanHpU+gH5BbiNhoJgrPSdlnfYkS8bnXkbHZWvKB09Io/zHx\niLpLEhGpvcnj3IhaiyUADB4MiNVkjNixslnTIbGnjwvdS7AmWGBAH3zSa8vsDM6I\nHyGoIuN841CAPLSGdp5OPAJ9O7VN6Mk7JAfK753uS7xODYzZ3YWzTN2uzEQp9ddA\nHAjeuMCPAgMBAAECggEANXQvLZyImWe3Kzqz68hL5XYdjCWvZEnpLZP2dNKPH7mc\nTYYJGEBXDkg0hipBraIWwtKHGn1keNB+LV5sShfrLZVFr8i2KitPwDyqNvxAlADx\nfJLelU6ozQ29KfB+pESmPzpLJ2kSaeuAWDyWQeqh/FvUGGCgiCd6U2r59ib2BRy3\n974g1JwVr1s9fPDmSaBGsrfsopBruu1xldGKJoJL69DNd1Q6GlAvLtopcTpM3BZY\nWq5gzBAJVL7xTzsJTCAfL/MedMLVakBt2w6S6vyuj7ctLU6EIvJOTBzqDxWBVHpr\nFHH2uEFLabPAaxEdF3AhuL2EjyjTAVTvUhnaUwlW4QKBgQDzn7KsInBh+a1wsC73\ngV6xlhNelE/YnC6yTO9S7a33QdNJXjXZW1pZeTx65+qx1lDtH9mtSnD8EY3Sm9V7\nvdiLN8qfg+hcVd5xCk4E38KqRB39fc8K/p8xMDCiIxy1f/lxGMLnHAZUvtr+O9e/\ncLG+XYVH6upS9ovXOpB5Ovdl9QKBgQC8KQt3Io1v85WeiJJ1XMK5qvw3GDZFS8qh\nNAZuOIl+xLULicOOnek5qi0P6mr5cTH4OQpM0pW8Rn7DTYxJJW6RkGNU2u1zSeVm\ngDgKmZaV6ZvA+L/Rms8xQolVNBMqLLpUHnCvEvy1cJBGv8DECNj1wAq4uFl19jpp\n3itaSuJ18wJ/QanJfpvJrp6dIMJb5ln4K+VHUzamTrvJ2kTiPHfTa9FSIXRDD1KQ\nB42lv3rCxyv8o+zxvsNRsJ8Kmrll0PRaaSugcV4cQsbiLZWZcbbdwQabDrfaFhyK\ny7cxZIISOtlDYjhKUAA9tJm8bMm0XOUDA0wxoDCw96t8BYbDael6xQKBgE5EkIVc\n9RJ1c24/hxSUoldTHZjZVHHcxgvqNSaSE/eMYXbMNnFTlFktRZNSQm9CO7PyHiu4\niRqJBF+/GTSwAl9AfWEltBH50heiDC20l6QCSYyqrDfHOppWAARWJgasFlG5W8wn\njMD4crtLNicVOxJ2cL9Hx8a+xquVA9mKPssrAoGAX4gcLYyu3VVknHZq/nKvIT3p\n+aiLLHQUSviLgHiraiParZgutjC0RBD9MVJmSXE3zuT2fiyRendzkM6bxyjc3JDE\nFJyRRhFTnHiFHm2ZqySZmDF7HEa53lW6yCV8YQZanHtudKt7ay9kVD9IlQ8KsyY6\nQ/8QfG69WL+EK5I8mXg=\n-----END PRIVATE KEY-----\n",
  "client_email": "starting-account-k20u6vuffj5c@ga4-python-1698391628860.iam.gserviceaccount.com",
  "token_uri": "https://oauth2.googleapis.com/token"
}

json_acct_info = cred_dict

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
