import pandas as pd
from google.cloud import storage
import pandas_gbq
import numpy as np
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
    sheet_id = ''
    sheet_name = ''
    url = f'https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}'
    df_adver = pd.read_csv(url)

  ## Variables
    agencies = [
        {'agency_name': '', 'id': ''},
        {'agency_name': '', 'id': ''},
        {'agency_name': '', 'id': ''}
    ]

    month = {
        'January': '01', 'February': '02', 'March': '03', 'April': '04', 'May': '05', 'June': '06',
        'July': '07', 'August': '08', 'September': '09', 'October': '10', 'November': '11', 'December': '12',
    }

    df.rename(columns={'Advertiser ID': 'account_id', 'Advertiser name': 'account_name'}, inplace=True)

    ## join to get advertiser && industry
    df = df.merge(df_adver, on='account_name', how='left')

    ## insert new columns
    df['media_platform'] = 'TikTok'
    df['agency_id'] = df['Agency'].map(dict((d['agency_name'], d['id']) for d in agencies))

    ori_cols = list(df.columns)
    new_cols = [col.strip().replace(' ','_').replace('\t','').replace('(','').replace(')','').replace('Video','vdo').replace('%','pct').lower() for col in ori_cols]
    df.rename(columns=dict(zip(ori_cols, new_cols)), inplace=True)

    # map month names to numbers
    df['month'] = df['month'].map(month).astype(str).apply(lambda x: '0' + x if len(x) == 1 else x)

    if 'age' in df.columns:
        df = df.reindex(columns=['media_platform', 'agency_id', 'agency', 'advertiser', 'industry', 'year', 'month', 
                                'account_id','account_name', 'campaign_id', 'campaign_name', 'objective',
                                'adgroup_id', 'adgroup_name', 'age', 'gender', 'bid_type','placement_type', 'currency', 
                                'cost', 'impressions', 'clicks', 'conversions', '2s_vdo', '6s_vdo', 'vdo_views', 
                                'vdo_views_at_25pct','vdo_views_at_50pct', 'vdo_views_at_75pct', 'vdo_views_at_100pct','complete_views'])
    
        df['age'] = df['age'].astype(str).apply(lambda x: x.replace('AGE_','').replace('_','-') if 'AGE' in x else x )

    if 'device_brand' in df.columns:
        df = df.reindex(columns=['media_platform', 'agency_id', 'agency', 'advertiser', 'industry', 'year', 'month', 
                                'account_id','account_name', 'campaign_id', 'campaign_name', 'objective',
                                'adgroup_id', 'adgroup_name', 'device_brand', 'bid_type','placement_type', 'currency', 
                                'cost', 'impressions', 'clicks', 'conversions', '2s_vdo', '6s_vdo', 'vdo_views', 
                                'vdo_views_at_25pct','vdo_views_at_50pct', 'vdo_views_at_75pct', 'vdo_views_at_100pct','complete_views'])

    # convert to string and replace with np.nan value if found 'nan', 'Null', or 'NaN'
    df.loc[:, :'currency'] = df.loc[:, :'currency'].astype(str)
    df.loc[:, :'currency'] = df.loc[:, :'currency'].replace('nan', np.nan).replace('Null', np.nan).replace('NaN',np.nan)

    # convert to float
    df.loc[:,'cost':] = df.loc[:,'cost':].astype('float64')

    return df

def export_to_gbq(df):
    if 'age' in df.columns:
        pandas_gbq.to_gbq(df, 'dataset.table', project_id='', if_exists='append')
    if 'device_brand' in df.columns:
        pandas_gbq.to_gbq(df, 'dataset.table', project_id='', if_exists='append')

def etl_data(event, context):
    data = get_data(event, context)
    cleaned_data = clean_data(data)
    export_to_gbq(cleaned_data)