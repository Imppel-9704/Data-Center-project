import pandas as pd
from google.cloud import storage
import pandas_gbq
import os
import io
import chardet
import numpy as np

def get_data(event, context):
    bucket_name = event['bucket']
    file_name = event['name']

    # Check if the file has a .csv extension
    if file_name.endswith('.csv'):
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)

        # Download and create dataframe
        blob_content = blob.download_as_bytes()
        enc = chardet.detect(blob_content)
        df = pd.read_csv(io.BytesIO(blob_content), encoding=enc['encoding'], sep='\t')

        return df, file_name

def clean_data(df, file_name):
    agencies = {
        'name01': {'business_id': '', 'agency_id': '', 'agency': ''},
        'name02': {'business_id': '', 'agency_id': '', 'agency': ''},
        'name03': {'business_id': '', 'agency_id': '', 'agency': ''},
    }

    sheet_id = ''
    sheet_name = ''
    url = f'https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}'
    line_adver = pd.read_csv(url)

    col_to_keep = ['Ad account ID', 'Day', 'Ad account name', 'Campaign name', 'Campaign ID', 'Campaign objective', 
                    'Ad group name', 'Ad group ID', 'Ad name', 'Ad ID', 
                    'Impressions', 'Viewable impressions', 'Clicks', 'Cost', 'Currency', 'Reach (estimated)', 'Frequency', 'Video starts', 
                    'Video (viewed for at least three seconds)', 'Cost per 3-second playback', 'Video (25% watched)', 'Video (50% watched)', 
                    'Video (75% watched)', 'Video (95% watched)', 'Video (100% watched)', 'Installs (clicks and views)', 'Installs (clicks)',
                    'Installs (views)', 'Purchase', 'CPA (cost per action)']
  
    for agency_name, agency_data in agencies.items():
        if agency_name in file_name:

            if not df.empty:
                ## drop difference columns
                df = df.drop(columns = df.columns.difference(col_to_keep))
                df['business_id'] = agency_data['business_id']
                df['media_platform'] = 'Line Ads'
                df['agency_id'] = agency_data['agency_id']
                df['agency'] = agency_data['agency']

                ## join data with google sheet to get advertiser and industry
                df = pd.merge(df, line_adver, how= "left", left_on= 'Ad account name', right_on='account_name')

                ## rename, replace columns
                df.rename(columns= {'Video (viewed for at least three seconds)':'vdo_view_at_least_3_secs',
                                        'Installs (clicks and views)':'installs_clicks_views',
                                        'Cost per 3-second playback':'cost_per_3sec_playback',
                                        'Day':'month'}, inplace=True)
                ori_cols = list(df.columns)
                new_cols = [i.strip().replace(' ','_').replace('\t','').replace('(','').replace(')','')\
                            .replace('Video','vdo').replace('%','pct').lower() for i in ori_cols] 
                df.rename(columns= dict(zip(ori_cols, new_cols)), inplace=True)

                ## drop unnecessary column
                df.drop(columns='account_name', inplace=True)

                ## insert year and month column
                df['year'] = df['month'].str[:4]
                df['month'] = df['month'].str[5:7]

                ## reindex
                df = df.reindex(columns=['business_id', 'media_platform', 'agency_id', 'agency', 'advertiser', 'industry', 
                                        'year', 'month', 'ad_account_id', 'ad_account_name',  'campaign_id', 'campaign_name',
                                        'campaign_objective', 'ad_group_id', 'ad_group_name', 'ad_id', 'ad_name', 'currency', 
                                        'cost','impressions', 'viewable_impressions', 'clicks', 'reach_estimated', 'frequency', 
                                        'vdo_starts', 'vdo_view_at_least_3_secs', 'cost_per_3sec_playback', 'vdo_25pct_watched', 
                                        'vdo_50pct_watched', 'vdo_75pct_watched', 'vdo_95pct_watched', 'vdo_100pct_watched', 
                                        'installs_clicks_views', 'installs_clicks', 'installs_views', 'purchase','cpa_cost_per_action'])
                
                # str cols 
                df.loc[:, :'currency'] = df.loc[:, :'currency'].astype(str)
                df.loc[:, :'currency'] = df.loc[:, :'currency'].replace('nan', np.nan).replace('Null', np.nan).replace('NaN',np.nan)
                
                # float cols
                float_cols = ['cost','impressions', 'viewable_impressions', 'clicks', 'reach_estimated', 'frequency', 
                                'vdo_starts', 'vdo_view_at_least_3_secs', 'cost_per_3sec_playback', 'vdo_25pct_watched', 
                                'vdo_50pct_watched', 'vdo_75pct_watched', 'vdo_95pct_watched', 'vdo_100pct_watched', 
                                'installs_clicks_views', 'installs_clicks', 'installs_views', 'purchase','cpa_cost_per_action']
                df[float_cols] = df[float_cols].astype('float64')

                return df
                
            else:
                return pd.DataFrame()

def export_to_gbq(df):
    pandas_gbq.to_gbq(df, 'dataset.table', project_id='', if_exists='append')


def etl_data(event, context):
    data, file_name = get_data(event, context)
    cleaned_data = clean_data(data, file_name=event['name'])
    if not cleaned_data.empty:
        export_to_gbq(cleaned_data)
    else:
        print(f'{file_name} is empty')