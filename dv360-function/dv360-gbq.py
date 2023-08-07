import pandas as pd
from google.cloud import storage
import pandas_gbq
import os
import io

def get_data(event, context):
    bucket_name = event['bucket']
    file_name = event['name']

  # Check if the file has a .csv extension
    if file_name.endswith('.csv'):
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)

        # Download and create dataframe
        blob_content = blob.download_as_text()
        df = pd.read_csv(io.StringIO(blob_content))

        return df

def clean_data(df):
  ## Google Sheet
    SHEET_ID = ''
    SHEET_NAME = ''
    url = f'https://docs.google.com/spreadsheets/d/{SHEET_ID}/gviz/tq?tqx=out:csv&sheet={SHEET_NAME}'
    adver = pd.read_csv(url)

  ## Variables
    agencies = [
        {'agency': '', 'agency_id': ''},
        {'agency': '', 'agency_id': ''},
        {'agency': '', 'agency_id': ''},
    ]

    replace_word = {'DX': 'agency_name', 'CA': 'agency_name', 'FL': 'agency_name', 'VZ': 'agency_name', 'IP': 'agency_name', 'PO': 'agency_name', 'DT': 'agency_name', 'DM': 'agency_name'}
    col_check = ['Partner ID', 'Partner', 'Advertiser ID', 'Advertiser', 'Advertiser Currency', 'Campaign ID', 'Campaign', 'Insertion Order ID', 'Insertion Order', 
                 'Line Item ID', 'Line Item', 'Year', 'Month', 'Bid Strategy Type', 'Creative Type', 'Device Type', 'Media Type', 'Impressions', 'Clicks', 'Media Cost (Advertiser Currency)', 
                 'Total Media Cost (Advertiser Currency)', 'First-Quartile Views (Video)', 'Midpoint Views (Video)', 'Third-Quartile Views (Video)', 'Complete Views (Video)', 'Active View: Viewable Impressions']

    ## make sure our data is correct by checking column quantity and column name
    if len(df.columns.tolist()) == 28:
        if set(col_check).issubset(df.columns):

            ## drop total summary row
            df.drop(df.tail(25).index, inplace=True)

            ## rename columns
            df.rename(columns={'Partner ID': 'business_id', 'Partner': 'business','Advertiser': 'account_name', 'Advertiser ID': 'account_id'}, inplace=True)

            ## join data with google sheet to get advertiser and industry
            df = pd.merge(df, adver, how= "left", left_on= "account_name", right_on= "account_name")

            ## get list of column name then rename
            ori_col = list(df.columns)
            new_col = [col.strip().replace(' ', '_').replace('\t','').replace("-", "_").replace("/", "_").replace('(','').replace(')','').replace(":", "").lower() for col in ori_col]
            df.rename(columns=dict(zip(ori_col, new_col)), inplace=True)

            ## insert new column
            df.insert(0, 'media_platform', 'DV360')

            ## if it extract words in condition. It will replace with agency_name from replace_word in agency column
            df['agency'] = df['account_name'].str.extract(r"(?<=THB\\)(DX|CA|FL|VZ|IP|PO|DT|DM)").replace(replace_word, regex=True)
            df['agency_id'] = df['agency'].str.lower().map({agency['agency'].lower(): agency['agency_id'] for agency in agencies.values()})
            df['month'] = df['month'].str.extract("/([0-9]{2})")

            ## change columns position
            df = df.reindex(columns=['media_platform', 'business_id', 'business', 'agency_id', 'agency', 'advertiser', 'industry', 'account_id', 'account_name', 'advertiser_currency', 'campaign_id', 'campaign',
                        'insertion_order_id', 'insertion_order', 'line_item_id', 'line_item', 'app_url_id', 'app_url', 'year', 'month', 'bid_strategy_type', 'creative_type', 'device_type', 'media_type', 
                        'impressions', 'clicks', 'media_cost_advertiser_currency', 'total_media_cost_advertiser_currency', 'first_quartile_views_video', 'midpoint_views_video', 
                        'third_quartile_views_video', 'complete_views_video', 'active_view_viewable_impressions'])

            df.loc[:, :'media_type'] = df.loc[:, :'media_type'].astype(str)
            df.loc[:, 'business_id':'year'] = df.loc[:, 'business_id':'year'].apply(lambda x: x.str.rstrip('.0'))

            df.loc[:, 'impressions':'active_view_viewable_impressions'] = df.loc[:, 'impressions':'active_view_viewable_impressions'].astype('float64')

    return df

def export_to_gbq(df):
    pandas_gbq.to_gbq(df, 'dataset.table', project_id='', if_exists='append')


def etl_data(event, context):
    data = get_data(event, context)
    cleaned_data = clean_data(data)
    export_to_gbq(cleaned_data)