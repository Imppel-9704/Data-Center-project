import pandas as pd
import numpy as np
from google.cloud import storage
import os
import io
import zipfile
import pandas_gbq

tmp_path = "/tmp/"

# function extract zip file
def extract_zip(blob):
    with zipfile.ZipFile(io.BytesIO(blob.download_as_string())) as zip_ref:
        zip_ref.extractall(tmp_path)
        extracted_files = zip_ref.namelist()
    return extracted_files

# download file from bucket
def download_file(event, context):
    bucket_name = event['bucket']
    file_name = event['name']
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    files = extract_zip(blob)
    return files

# function clean fb data
def clean_data(df, file_name, fb_adver):
    agencies = {
        'name1': {'business_id': '', 'agency_id': '', 'agency': ''},
        'name2': {'business_id': '', 'agency_id': '', 'agency': ''},
        'name3': {'business_id': '', 'agency_id': '', 'agency': ''},
    }

    for agency_name, agency_data in agencies.items():
        if agency_name in file_name and df is not None:
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
            if 'landing page view' in df.columns:
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

                dims = ['business_id', 'media_platform', 'agency_id', 'agency', 'account_id', 'account_name', 'campaign_id', 'campaign_name', 'year', 'month', 'gender', 'age',
                        'ad_set_id', 'ad_set_name', 'ad_id', 'ad_name', 'advertiser', 'industry', 'currency', 'objective']
                metrics = ['impressions', 'amount_spent_thb', 'link_clicks', 'page_likes', '3-second_video_plays', 'landing_page_views', 'app_installs', 'meta_leads', 'mobile_app_adds_to_cart',
                        'website_adds_to_cart', 'mobile_app_content_views', 'website_content_views', 'mobile_app_purchases', 'website_purchases', 'mobile_app_adds_to_cart_conversion_value',
                        'website_adds_to_cart_conversion_value', 'mobile_app_purchases_conversion_value', 'website_purchases_conversion_value', 'leads', 'clicks_all', 'post_engagement']
                df = df.groupby(dims).sum()[metrics].reset_index()

            elif 'device_platform' in df.columns:
                df = df.reindex(columns=[
                'business_id', 'media_platform', 'agency_id', 'agency', 'account_id', 'account_name', 'campaign_id', 'campaign_name', 'year', 'month', 'platform', 'placement', 'device_platform', 
                'ad_set_id', 'ad_set_name', 'ad_id', 'ad_name', 'advertiser', 'industry', 'currency', 'objective', 
                'impressions', 'amount_spent_thb', 'link_clicks', 'page_likes', '3-second_video_plays', 'landing_page_views', 'app_installs', 'meta_leads', 'mobile_app_adds_to_cart', 
                'website_adds_to_cart', 'mobile_app_content_views', 'website_content_views', 'mobile_app_purchases', 'website_purchases', 'mobile_app_adds_to_cart_conversion_value', 
                'website_adds_to_cart_conversion_value', 'mobile_app_purchases_conversion_value', 'website_purchases_conversion_value', 'leads', 'clicks_all', 'post_engagement'
                ])

                dims = ['business_id', 'media_platform', 'agency_id', 'agency', 'account_id', 'account_name', 'campaign_id', 'campaign_name', 'year', 'month', 'platform', 'placement', 'device_platform', 
                        'ad_set_id', 'ad_set_name', 'ad_id', 'ad_name', 'advertiser', 'industry', 'currency', 'objective']
                metrics = ['impressions', 'amount_spent_thb', 'link_clicks', 'page_likes', '3-second_video_plays', 'landing_page_views', 'app_installs', 'meta_leads', 'mobile_app_adds_to_cart', 
                        'website_adds_to_cart', 'mobile_app_content_views', 'website_content_views', 'mobile_app_purchases', 'website_purchases', 'mobile_app_adds_to_cart_conversion_value', 
                        'website_adds_to_cart_conversion_value', 'mobile_app_purchases_conversion_value', 'website_purchases_conversion_value', 'leads', 'clicks_all', 'post_engagement']
                df = df.groupby(dims).sum()[metrics].reset_index()

            df.loc[:, :'objective'] = df.loc[:, :'objective'].astype(str)
            df.loc[:, :'objective'] = df.loc[:, :'objective'].replace('nan', '').replace('Null', '').replace('NaN','')
            df.loc[:, 'impressions':] = df.loc[:, 'impressions':].astype('float64')
            return df

# get fb data, convert it as dataframe, clean data
def get_fb(files):

    SHEET_ID = ''
    SHEET_NAME = ''
    url = f'https://docs.google.com/spreadsheets/d/{SHEET_ID}/gviz/tq?tqx=out:csv&sheet={SHEET_NAME}'
    fb_adver = pd.read_csv(url)

    # dfs = [clean_data(pd.read_excel(f"/tmp/{file}"), file, fb_adver)
    #         for file in files if "reach" not in file.lower() and file.endswith(".xlsx")]
    
    dfs = list(map(lambda file: clean_data(pd.read_excel(f"{tmp_path}/{file}"), file, fb_adver),
            filter(lambda file: "reach" not in file.lower() and file.endswith(".xlsx"), files)))

    return dfs

# get fb add-on data as dataframe and clean data
def get_ao(files):
    # dfs = [pd.read_csv(f"/tmp/{file}")
    #         for file in files if file.endswith(".csv")]
    
    dfs = list(map(lambda file: pd.read_csv(f"{tmp_path}/{file}"),
            filter(lambda file: file.endswith(".csv"), files)))
    ag_df, dpp_df = clean_ao(dfs)

    return ag_df, dpp_df

# function for clean fb add-on data
def clean_ao(dfs):
    ag_df = pd.DataFrame()
    dpp_df = pd.DataFrame()

    for df in dfs:
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

# loop dataframe to split into 2 breakdown then append df to list
def join_data(dfs, ag_df, dpp_df):
    final_result = []

    for df in dfs:
        if 'age' in df.columns:
            final_df = df.merge(ag_df, how='left',
                                on=['account_id', 'account_name', 'campaign_id', 'campaign_name', 'objective', 
                                    'year', 'month', 'gender', 'age', 'ad_set_id', 'ad_set_name', 'ad_id', 'ad_name'])
            
            final_df.rename(columns={'impressions_x': 'impressions', 'impressions_y': 'impressions_addon', 'leads_x': 'leads', 
                                    'leads_y': 'leads_add-on', '3-second_video_plays':'sec3_video_plays', 'video_thruplay':'thruplay_actions'}, inplace=True)

            final_df = final_df.reindex(columns=['business_id', 'media_platform', 'agency_id', 'agency', 'account_id', 'account_name', 'campaign_id', 'campaign_name', 'ad_set_id', 'ad_set_name', 
                                                'ad_id', 'ad_name', 'year', 'month','gender', 'age', 'advertiser', 'industry', 'objective', 'currency', 'ad_creative_object_type',
                                                'impressions','amount_spent_thb', 'link_clicks', 'page_likes', 'sec3_video_plays', 'landing_page_views', 'app_installs', 'meta_leads', 'mobile_app_adds_to_cart', 
                                                'website_adds_to_cart', 'mobile_app_content_views', 'website_content_views', 'mobile_app_purchases', 'website_purchases', 'mobile_app_adds_to_cart_conversion_value', 
                                                'website_adds_to_cart_conversion_value', 'mobile_app_purchases_conversion_value', 'website_purchases_conversion_value', 'leads', 'clicks_all', 'post_engagement', 
                                                'impressions_addon', 'event_responses', 'outbound_clicks', 'leads_add-on', 'thruplay_actions'])
            final_result.append(final_df)
      
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
            final_result.append(final_df)
    
        ## add blank value to columns that originally have in bigquery table (old data)
        final_df['impressions_addon'] = final_df['impressions'].astype('float64')
        
        new_cols = ['omni_purchase_roas_shared_item',
                    'omni_adds_to_cart','omni_content_views','omni_app_purchases',
                    'omni_adds_to_cart_conversion_value','omni_purchases_conversion_value']
        final_df.loc[:,new_cols] = np.nan
        final_df.loc[:,new_cols] = final_df.loc[:,new_cols].astype('float64')

    return final_result

# export data
def export_data(final_result):
    for df in final_result:
        if 'age' in df.columns:
            pandas_gbq.to_gbq(df, 'project.dataset.table', project_id='project_id', if_exists='append')
            print("export age-gender complete")
        elif 'device' in df.columns:
            pandas_gbq.to_gbq(df, 'project.dataset.table', project_id='project_id', if_exists='append')
            print("export dpp complete")

# trigger all function above
def etl_data(event, context):
    files = download_file(event, context)
    data_fb = get_fb(files)
    data_ag, data_dpp = get_ao(files)
    final_data = join_data(data_fb, data_ag, data_dpp)
    export_data(final_data)