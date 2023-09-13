## New version of Google Ads function

import pandas as pd
from google.cloud import storage
import numpy as np
import io
import chardet
import zipfile
import pandas_gbq

tmp_path = "/tmp/"

# Extract zip file function
def extract_zip(blob):
  with zipfile.ZipFile(io.BytesIO(blob.download_as_string())) as zip_ref:
    zip_ref.extractall(tmp_path)
    extracted_files = zip_ref.namelist()
  return extracted_files

# Get files from bucket, download, read it as .csv, and append to list while doing data cleaning
def get_file(event, context):
  bucket_name = event['bucket']
  file_name = event['name']
  storage_client = storage.Client()
  bucket = storage_client.bucket(bucket_name)
  blob = bucket.blob(file_name)
  files = extract_zip(blob)

  sheet_id = ''
  sheet_name = ''
  url = f'https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}'
  df_adver = pd.read_csv(url)

  enc = chardet.detect(blob.download_as_string())

  dfs = [clean_data(pd.read_csv(f"/tmp/{file}", encoding=enc['encoding'], delimiter=",", skiprows=2), file, df_adver) 
        for file in filter(lambda file: file.endswith(".csv"), files)]
  
  return dfs

# concat files to single dataframe, export to BigQuery
def export_data(dfs):
  age_df = pd.DataFrame()
  type_df = pd.DataFrame()

  for df in dfs:
    if "age" in df.columns:
      age_df = pd.concat([age_df, df], ignore_index=True)
    elif "ad_type" in df.columns:
      type_df = pd.concat([type_df, df], ignore_index=True)

  pandas_gbq.to_gbq(age_df, 'table_age', project_id='diuhub-amplifith-acquisition', if_exists='append')
  print("export age complete")
  pandas_gbq.to_gbq(type_df, 'table_type', project_id='diuhub-amplifith-acquisition', if_exists='append')
  print("export device complete")

def clean_data(df, file_name, df_adver):
  agencies = {
    'name1': {'business_id': '', 'agency_id': '', 'agency': ''},
    'name2': {'business_id': '', 'agency_id': '', 'agency': ''},
    'name3': {'business_id': '', 'agency_id': '', 'agency': ''},
  }

  month_map = {
    'January': '01', 'February': '02', 'March': '03', 'April': '04', 'May': '05', 'June': '06', 
    'July': '07', 'August': '08', 'September': '09', 'October': '10', 'November': '11', 'December': '12',
    'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04', 'May': '05', 'Jun': '06',
    'Jul': '07', 'Aug': '08', 'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12',
  }

  ## create variables that match with file name
  if "agency_mcc" in file_name:
    business_id = agencies['agency_mcc']['business_id']
    agency_id = agencies['agency_mcc']['agency_id']
    agency = agencies['agency_mcc']['agency']

  elif "agency_advertiser" in file_name:
    business_id = agencies['agency_advertiser']['business_id']
    agency_id = agencies['agency_advertiser']['agency_id']
    agency = agencies['agency_advertiser']['agency']
        
  for agency_name, agency_data in agencies.items():
    if agency_name in file_name:
      business_id = agency_data['business_id']
      agency_id = agency_data['agency_id']
      agency = agency_data['agency']

  if 'Search keyword match type ' in df.columns:
    df = df.rename({'Search keyword match type ':'Search keyword match type'}, axis= 1)

  if 'Ad type' in df.columns:
    if 'advertiser' in file_name:
      df['Customer id'] = '111-111-1111'
      df['Account name'] = 'Agency (Advertiser)'
      df['Search keyword match type'] = df.apply(lambda x: 'Null', axis=1)
    elif 'Search keyword match type' not in df.columns:
      df['Search keyword match type'] = df.apply(lambda x: 'Null', axis=1)
        
  if 'Age' in df.columns and 'honda' in file_name:
    df['Customer_id'] = '111-111-1111'
    df['Account name'] = 'Agency (Advertiser)'

  ## join to get advertiser && industry
  df = pd.merge(df, df_adver, how='left', on='Account name')

  ## rename, replace columns
  ori_cols = list(df.columns)
  new_cols = [col.strip().replace(' ','_').replace('\t','').replace('(','').replace(')','').replace('.','').lower() for col in ori_cols]
  df.rename(columns=dict(zip(ori_cols, new_cols)), inplace=True)

  ## insert new columns
  extract_year = df['month'].str.extract(r"(\d{2})$")
  df['year'] = '20' + extract_year[0]
  df['month'] = df['month'].str.split('-| ').str[0].map(month_map)
  df['media_platform'] = 'Google Ads'
  df['business_id'] = business_id
  df['agency_id'] = agency_id
  df['agency'] = agency

  if 'ad_type' in df.columns:
    df = df.reindex(columns=['business_id', 'media_platform', 'agency_id', 'agency', 'advertiser', 'industry', 'customer_id',
                            'account_name','campaign_id', 'campaign', 'year', 'month', 'campaign_type',
                            'network_with_search_partners', 'campaign_bid_strategy_type',
                            'ad_group_type', 'ad_type', 'search_keyword_match_type', 'device', 'currency_code',
                            'impr', 'clicks', 'cost', 'conversions', 'views', 'view_rate'])

  if 'age' in df.columns:
    df = df.reindex(columns=['business_id', 'media_platform', 'agency_id', 'agency', 'advertiser', 'industry', 'customer_id',
                            'account_name', 'campaign_id', 'campaign', 'year', 'month','campaign_type',
                            'network_with_search_partners', 'campaign_bid_strategy_type',
                            'ad_group_type', 'age', 'gender', 'device', 'currency_code',
                            'impr', 'clicks', 'cost', 'conversions', 'views', 'view_rate'])

  ## convert to str
  df.loc[:, :'currency_code'] = df.loc[:, :'currency_code'].astype(str)
  df.loc[:, :'ad_group_type'] = df.loc[:, :'ad_group_type'].replace('nan', np.nan).replace('Null', np.nan).replace('NaN',np.nan)

  ## convert to str for using strip function
  df.loc[:, 'impr':'view_rate'] = df.loc[:, 'impr':'view_rate'].astype('str')

  ## strip, replace unnecessary words
  df.loc[:, 'impr':] = df.loc[:, 'impr':].applymap(lambda x: x.strip('"').replace(',', '').replace('%', ''))
  df.loc[:, 'impr':'view_rate'] = df.loc[:, 'impr':'view_rate'].replace('nan', np.nan)

  ## convert to float
  float_cols = ['impr', 'clicks', 'cost', 'conversions', 'views', 'view_rate']
  df[float_cols] = df[float_cols].astype('float64')

  return df

def etl_data(event, context):
  data = get_file(event, context)
  export_data(data)
