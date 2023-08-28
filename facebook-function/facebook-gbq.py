import pandas as pd
import numpy as np
from google.cloud import storage
import pandas_gbq
import os
import io
import datetime as dt
from datetime import datetime, timedelta


# get facebook data and read it as excel file
def get_data(event, context):
    bucket_name = event['bucket']
    file_name = event['name']
    # Check if the file has a .xlsx extension
    if file_name.endswith('.xlsx'):
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        # Download and create dataframe
        blob_content = blob.download_as_bytes()
        df = pd.read_excel(io.BytesIO(blob_content))
        return df, file_name
  
    # function will read .csv file, to prevent error return it as None
    else:
        return None, None


def clean_data(df, file_name):
    agencies = {
        'name1': {'business_id': '', 'agency_id': '', 'agency': ''},
        'name2': {'business_id': '', 'agency_id': '', 'agency': ''},
        'name3': {'business_id': '', 'agency_id': '', 'agency': ''},
    }

    ## import advertiser from google sheet
    SHEET_ID = ''
    SHEET_NAME = ''
    url = f'https://docs.google.com/spreadsheets/d/{SHEET_ID}/gviz/tq?tqx=out:csv&sheet={SHEET_NAME}'
    fb_adver = pd.read_csv(url)

    for agency_name, agency_data in agencies.items():
        if agency_name in file_name:
            business_id = agency_data['business_id']
            agency_id = agency_data['agency_id']
            agency = agency_data['agency']
            extract_year = df['Month'].str.extract("([0-9]{4})")

            df.drop(df.columns[-2:], axis=1, inplace=True)
            df['business_id'] = business_id
            df['media_platform'] = 'Facebook'
            df['agency_id'] = agency_id
            df['agency'] = agency
            df['year'] = extract_year
            df['Month'] = df['Month'].str[5:7].astype(str).apply(lambda x: '0'+x if len(x) == 1 else x )

            # Join data with google sheet
            df = df.merge(fb_adver, how='left', on='Account name')

            # Rename columns if needed
            if 'Amount spent' in df.columns:
                df.rename(columns={'Amount spent': 'Amount spent THB'}, inplace=True)
            elif 'landing page view' in df.columns:
                df.rename(columns={'landing page view': 'Landing page views'}, inplace=True)

    
            df.rename(columns={col: col.strip().replace(' ', '_').replace('/t', '').replace('(', '').replace(')', '').lower()
                    for col in df.columns}, inplace=True)

            if 'age' in df.columns:
                df = df.reindex(columns=[
                'business_id', 'media_platform', 'agency_id', 'agency', 'account_id', 'account_name', 'campaign_id', 'campaign_name', 'year', 'month', 'gender', 'age',
                'ad_set_id', 'ad_set_name', 'ad_id', 'ad_name', 'advertiser', 'industry', 'currency', 'objective',
                'impressions', 'amount_spent_thb', 'link_clicks', 'page_likes', '3-second_video_plays', 'landing_page_views', 'app_installs', 'meta_leads', 'mobile_app_adds_to_cart',
                'website_adds_to_cart', 'mobile_app_content_views', 'website_content_views', 'mobile_app_purchases', 'website_purchases', 'mobile_app_adds_to_cart_conversion_value',
                'website_adds_to_cart_conversion_value', 'mobile_app_purchases_conversion_value', 'website_purchases_conversion_value', 'leads', 'clicks_all', 'post_engagement'
                ])

            elif 'device_platform' in df.columns:
                df = df.reindex(columns=[
                'business_id', 'media_platform', 'agency_id', 'agency', 'account_id', 'account_name', 'campaign_id', 'campaign_name', 'year', 'month', 'platform', 'placement', 'device_platform', 
                'ad_set_id', 'ad_set_name', 'ad_id', 'ad_name', 'advertiser', 'industry', 'currency', 'objective', 
                'impressions', 'amount_spent_thb', 'link_clicks', 'page_likes', '3-second_video_plays', 'landing_page_views', 'app_installs', 'meta_leads', 'mobile_app_adds_to_cart', 
                'website_adds_to_cart', 'mobile_app_content_views', 'website_content_views', 'mobile_app_purchases', 'website_purchases', 'mobile_app_adds_to_cart_conversion_value', 
                'website_adds_to_cart_conversion_value', 'mobile_app_purchases_conversion_value', 'website_purchases_conversion_value', 'leads', 'clicks_all', 'post_engagement'
                ])

            df.loc[:, :'objective'] = df.loc[:, :'objective'].astype(str)
            df.loc[:, :'objective'] = df.loc[:, :'objective'].replace('nan', '').replace('Null', '').replace('NaN','')
            df.loc[:, 'impressions':] = df.loc[:, 'impressions':].astype('float64')
            return df


def get_ao(event, context):
    bucket_name = event['bucket']
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs()

    df_list = [
        pd.read_csv(io.BytesIO(blob.download_as_bytes()))
        for blob in blobs
        if blob.name.endswith('.csv') and blob.updated.replace(tzinfo=None) + dt.timedelta(hours=7) >= datetime.now() - timedelta(days=7)
    ]
    return df_list


def clean_ao(df_list):
    ag_df = pd.DataFrame()
    dpp_df = pd.DataFrame()

    for df in df_list:
        ## rename to lower and replace white spaces with underscore
        df.columns = map(str.lower, df.columns)
        df.columns = df.columns.str.replace(' ', '_')
        ## rename columns
        df.rename(columns={'campaign_objective': 'objective', 'account': 'account_name', 'creative_object_type': 'ad_creative_object_type'}, inplace=True)
        ## convert data type
        df.loc[:, :'ad_name'] = df.loc[:, :'ad_name'].astype(str)
        df.loc[:, :'ad_name'] = df.loc[:, :'ad_name'].replace('nan', '').replace('Null', '').replace('NaN','')
        col_float = ['impressions', 'event_responses', 'outbound_clicks', 'leads', 'video_thruplay']
        df[col_float] = df[col_float].astype('float64')

        if 'age' in df.columns:
            ## concat df
            ag_df = pd.concat([ag_df, df])
            ## Add 0 in the front of month number
            ag_df['month'] = ag_df['month'].str.zfill(2)
            ag_df = ag_df.groupby(['account_id','campaign_id','ad_id','year','month','gender','age']).sum().reset_index()

        elif 'device_platform' in df.columns:
            ## concat df
            dpp_df = pd.concat([dpp_df, df])
            ## Add 0 in the front of month number
            dpp_df['month'] = dpp_df['month'].str.zfill(2)
            dpp_df = dpp_df.groupby(['account_id','campaign_id','ad_id','year','platform', 'placement', 'device_platform']).sum().reset_index()
  
    return ag_df, dpp_df
    

def join_data(df, ag_df, dpp_df):
    if df is not None:
        if 'age' in df.columns:
            final_df = df.merge(ag_df, how='left',
                            on=['account_id', 'account_name', 'campaign_id', 'campaign_name', 'objective', 'year', 'month',
                                'gender', 'age', 'ad_set_id', 'ad_set_name', 'ad_id', 'ad_name'])
            
            final_df.rename(columns={'impressions_x': 'impressions', 'impressions_y': 'impressions_addon', 'leads_x': 'leads', 
                                    'leads_y': 'leads_add-on', '3-second_video_plays':'sec3_video_plays', 'video_thruplay':'thruplay_actions'}, inplace=True)

            final_df = final_df.reindex(columns=['business_id', 'media_platform', 'agency_id', 'agency', 'account_id', 'account_name', 'campaign_id', 'campaign_name', 'ad_set_id', 'ad_set_name', 
                                                'ad_id', 'ad_name', 'year', 'month','gender', 'age', 'advertiser', 'industry', 'objective', 'currency', 'ad_creative_object_type',
                                                'impressions','amount_spent_thb', 'link_clicks', 'page_likes', 'sec3_video_plays', 'landing_page_views', 'app_installs', 'meta_leads', 'mobile_app_adds_to_cart', 
                                                'website_adds_to_cart', 'mobile_app_content_views', 'website_content_views', 'mobile_app_purchases', 'website_purchases', 'mobile_app_adds_to_cart_conversion_value', 
                                                'website_adds_to_cart_conversion_value', 'mobile_app_purchases_conversion_value', 'website_purchases_conversion_value', 'leads', 'clicks_all', 'post_engagement', 
                                                'impressions_addon', 'event_responses', 'outbound_clicks', 'leads_add-on', 'thruplay_actions'])
            
        elif 'device_platform' in df.columns:
            final_df = df.merge(dpp_df, how='left',
                                on=['account_id', 'account_name', 'campaign_id', 'campaign_name', 'ad_set_id', 'ad_set_name', 
                                    'ad_id', 'ad_name', 'year', 'month', 'platform', 'placement', 'device_platform', 'objective'])
            
            final_df.rename(columns={'impressions_x': 'impressions', 'impressions_y': 'impressions_addon', 'leads_x': 'leads', 
                                    'leads_y': 'leads_add-on', '3-second_video_plays':'sec3_video_plays', 'video_thruplay':'thruplay_actions'}, inplace=True)

            final_df = final_df.reindex(columns=['business_id', 'media_platform', 'agency_id', 'agency', 'account_id', 'account_name', 'campaign_id', 'campaign_name', 'ad_set_id', 'ad_set_name', 
                                                'ad_id', 'ad_name', 'year', 'month', 'platform', 'placement', 'device_platform', 'advertiser', 'industry', 'objective', 'currency', 'ad_creative_object_type',
                                                'impressions','amount_spent_thb', 'link_clicks', 'page_likes', 'sec3_video_plays', 'landing_page_views', 'app_installs', 'meta_leads', 'mobile_app_adds_to_cart', 
                                                'website_adds_to_cart', 'mobile_app_content_views', 'website_content_views', 'mobile_app_purchases', 'website_purchases', 'mobile_app_adds_to_cart_conversion_value', 
                                                'website_adds_to_cart_conversion_value', 'mobile_app_purchases_conversion_value', 'website_purchases_conversion_value', 'leads', 'clicks_all', 'post_engagement', 
                                                'impressions_addon', 'event_responses', 'outbound_clicks', 'leads_add-on', 'thruplay_actions'])
        
        ## add blank value to columns that originally have in bigquery table (old data)
        final_df['impressions_addon'] = final_df['impressions'].astype('float64')
        
        new_cols = ['omni_purchase_roas_shared_item',
                    'omni_adds_to_cart','omni_content_views','omni_app_purchases',
                    'omni_adds_to_cart_conversion_value','omni_purchases_conversion_value']
        final_df.loc[:,new_cols] = np.nan
        final_df.loc[:,new_cols] = final_df.loc[:,new_cols].astype('float64')
        return final_df


def export_data(final_df):
    if final_df is not None:
        if 'age' in final_df.columns:
            pandas_gbq.to_gbq(final_df, 'project_name.dataset.table_age-gender', project_id='project_id', if_exists='append')
        elif 'device_platform' in final_df.columns:
            pandas_gbq.to_gbq(final_df, 'project_name.dataset.table_dpp', project_id='project_id', if_exists='append')


def etl_data(event, context):
    data, file_name = get_data(event, context)
    cleaned_data = clean_data(data, file_name=event['name'])
    data_ao = get_ao(event, context)
    cleaned_ag, cleaned_dpp = clean_ao(data_ao)
    final_data = join_data(cleaned_data, cleaned_ag, cleaned_dpp)
    export_data(final_data)