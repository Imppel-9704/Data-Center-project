import pandas as pd
import numpy as np
from google.cloud import storage
import os
import io
import zipfile
import pandas_gbq

# function extract zip file
def extract_zip(blob):
    with zipfile.ZipFile(io.BytesIO(blob.download_as_string())) as zip_ref:
        tmp_path = "/tmp/"
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

# function for clean data
def clean_data(df):
    df.columns = [col.replace(' ', '_').replace('\t', '').replace('(', '').replace(')', '').lower() for col in df.columns]
    df = df.astype({col: str for col in df.columns if 'id' in col})
    df.drop(df.columns[-2:], axis=1, inplace=True)
    return df

# get fb reach data, convert it as dataframe and concat data
def get_reach(files, file_filter_text):
    dfs = [pd.read_excel(f"/tmp/{file}")
        for file in files if file_filter_text.lower() in file.lower() and file.endswith('.xlsx')
        ]
    if not dfs:
        return None
    reach_df = pd.concat(dfs, ignore_index=True)
    return reach_df

# get fb data, convert it as dataframe, clean data and concat data
def get_fb(files):
    SHEET_ID = ''
    SHEET_NAME = ''
    url = f'https://docs.google.com/spreadsheets/d/{SHEET_ID}/gviz/tq?tqx=out:csv&sheet={SHEET_NAME}'
    fb_adver = pd.read_csv(url)

    dfs = [clean_df(pd.read_excel(f"/tmp/{file}"), file, fb_adver)
        for file in files if "agegender" in file.lower() and file.endswith(".xlsx")]

    df = pd.concat(dfs, ignore_index=True)
    return df

# function clean fb data
def clean_df(df, file_name, fb_adver):
    agencies = {
        'name1': {'business_id': '', 'agency_id': '', 'agency': ''},
        'name2': {'business_id': '', 'agency_id': '', 'agency': ''},
        'name3': {'business_id': '', 'agency_id': '', 'agency': ''},
    }

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
            if 'landing page view' in df.columns:
                df.rename(columns={'landing page view': 'Landing page views'}, inplace=True)

            df.rename(columns={col: col.strip().replace(' ', '_').replace('/t', '').replace('(', '').replace(')', '').lower()
                        for col in df.columns}, inplace=True)

            new_cols = ['omni_adds_to_cart','omni_content_views','omni_app_purchases',
                        'omni_adds_to_cart_conversion_value','omni_purchases_conversion_value']
            df.loc[:,new_cols] = np.nan

            dims = ['business_id', 'media_platform', 'agency_id', 'agency', 'advertiser', 'industry', 'account_id', 'account_name', 
                    'campaign_id', 'campaign_name', 'year', 'month']

            metrics = ['impressions', 'amount_spent_thb', 'link_clicks', 'page_likes',  'landing_page_views', 
                        'app_installs', 'meta_leads', 'mobile_app_adds_to_cart', 'website_adds_to_cart', 'mobile_app_content_views', 
                        'website_content_views', 'mobile_app_purchases', 'website_purchases', 'mobile_app_adds_to_cart_conversion_value',
                        'website_adds_to_cart_conversion_value', 'mobile_app_purchases_conversion_value', 'website_purchases_conversion_value', 
                        'leads', 'clicks_all', 'post_engagement', 'omni_adds_to_cart','omni_content_views',
                        'omni_app_purchases', 'omni_adds_to_cart_conversion_value','omni_purchases_conversion_value']

            df = df.groupby(dims).sum()[metrics].reset_index()

            df.loc[:, :'month'] = df.loc[:, :'month'].astype(str)
            df.loc[:, :'month'] = df.loc[:, :'month'].replace('nan', '').replace('Null', '').replace('NaN','')
            df.loc[:, 'impressions':] = df.loc[:, 'impressions':].astype('float64')

            return df

# join fb data and fb reach data
def join_data(df, rch_month, rch_camp):
    if rch_month is not None and rch_camp is not None:
        rch_month = clean_data(rch_month)
        rch_camp = clean_data(rch_camp)

        rch_month['year'] = rch_month['month'].apply(lambda x: str(x)[:4]) 
        rch_month['month'] = rch_month['month'].apply(lambda x: str(x)[5:7] if len(str(x)) > 2 else '0'+str(x) if len(str(x)) == 1 else str(x))

        rch_month = rch_month.loc[:,['account_id', 'account_name', 'campaign_id', 'year', 'month', 'reach', 'frequency']]
        rch_month = rch_month.rename(columns={'reach':'reach_cmp_monthly','frequency':'freq_cmp_monthly'})

        rch_camp = rch_camp.loc[:,['account_id','account_name','campaign_id','reach','frequency']]
        rch_camp = rch_camp.rename(columns={'reach':'reach_cmp','frequency':'freq_cmp'})

  # join df with rch_month & rch_camp
    if df is not None:
        final_df = df.merge(rch_month, on=['account_id','account_name','campaign_id','year','month'], how='left')
        final_df = final_df.merge(rch_camp, on=['account_id','account_name','campaign_id'], how='left')

        final_df['year'] = final_df['year'].astype(str) 

        #str cols 
        final_df.loc[:,:'month'] = final_df.loc[:,:'month'].astype(str)

        #floats cols 
        metr_cols = ['impressions', 'amount_spent_thb', 'link_clicks', 'page_likes', 'landing_page_views', 
                    'app_installs', 'meta_leads', 'mobile_app_adds_to_cart', 'website_adds_to_cart', 'mobile_app_content_views', 
                    'website_content_views', 'mobile_app_purchases', 'website_purchases', 'mobile_app_adds_to_cart_conversion_value',
                    'website_adds_to_cart_conversion_value', 'mobile_app_purchases_conversion_value', 'website_purchases_conversion_value',
                    'leads', 'clicks_all', 'post_engagement', 'omni_adds_to_cart', 'omni_content_views',
                    'omni_app_purchases', 'omni_adds_to_cart_conversion_value', 'omni_purchases_conversion_value', 'reach_cmp_monthly',
                    'freq_cmp_monthly', 'reach_cmp', 'freq_cmp']

    final_df[metr_cols] = final_df[metr_cols].astype('float64')

    final_df = final_df.drop_duplicates()

    return final_df

# export data
def export_data(final_df):
    if final_df is not None:
        pandas_gbq.to_gbq(final_df, 'project.dataset.table', project_id='project_id', if_exists='append')
        print("export facebook reach complete")

# trigger all above function
def etl_data(event, context):
    files = download_file(event, context)
    data = get_fb(files)
    rch_month  = get_reach(files, 'reach_monthly')
    rch_camp = get_reach(files, 'reach_camp')
    joined = join_data(data, rch_month, rch_camp)
    export_data(joined)