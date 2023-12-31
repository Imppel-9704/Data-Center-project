import pandas as pd
from google.cloud import storage
import pandas_gbq
import numpy as np
import io
import chardet

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
    df = pd.read_csv(io.BytesIO(blob_content), encoding=enc['encoding'], delimiter=',', skiprows=2)

    return df, file_name

def clean_data(df, file_name):
  sheet_id = ''
  sheet_name = ''
  url = f'https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}'
  df_adver = pd.read_csv(url)

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

  for agency_name, agency_data in agencies.items():
    if agency_name in file_name:
      ## create variables that match with file name
      business_id = agency_data['business_id']
      agency_id = agency_data['agency_id']
      agency = agency_data['agency']

        ## the data that is generated by google ads, some columns miss. so if it meet the condition do this
      if 'Ad type' in df.columns:
        if 'word' in file_name:
          df['Customer id'] = ''
          df['Account name'] = ''
          df['Search keyword match type'] = df.apply(lambda x: 'Null', axis=1)
        elif 'Search keyword match type' not in df.columns:
          df['Search keyword match type'] = df.apply(lambda x: 'Null', axis=1)
      
      if 'Age' in df.columns and 'word' in file_name:
        df['Customer_id'] = ''
        df['Account name'] = ''
  
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

      ## reindex
      if 'ad_type' in df.columns:
        df = df.reindex(columns=['business_id', 'media_platform', 'agency_id', 'agency', 'advertiser', 'industry',
                                'customer_id', 'account_name', 'campaign_id', 'campaign', 'year', 'month', 'campaign_type', 
                                'network_with_search_partners', 'campaign_bid_strategy_type', 'ad_group_type', 'ad_type',
                                'search_keyword_match_type', 'device', 'currency_code',
                                'impr', 'clicks', 'cost', 'conversions', 'views', 'view_rate'])
      if 'age' in df.columns:
        df = df.reindex(columns=['business_id', 'media_platform', 'agency_id', 'agency', 'advertiser', 'industry',
                                'customer_id', 'account_name', 'campaign_id', 'campaign', 'year', 'month', 'campaign_type', 
                                'network_with_search_partners', 'campaign_bid_strategy_type', 'ad_group_type',
                                'age', 'gender', 'device', 'currency_code',
                                'impr', 'clicks', 'cost', 'conversions', 'views', 'view_rate'])

      ## convert to str
      df.loc[:, :'currency_code'] = df.loc[:, :'currency_code'].astype(str)
      df.loc[:, :'currency_code'] = df.loc[:, :'currency_code'].replace('nan', np.nan).replace('Null', np.nan).replace('NaN',np.nan)

      ## convert to str for using strip function
      df.loc[:, 'impr':'view_rate'] = df.loc[:, 'impr':'view_rate'].astype('str')

      ## strip, replace unnecessary words
      df.loc[:, 'impr':] = df.loc[:, 'impr':].applymap(lambda x: x.strip('"').replace(',', '').replace('%', ''))
      df.loc[:, 'impr':'view_rate'] = df.loc[:, 'impr':'view_rate'].replace('nan', np.nan)

      ## convert to float
      float_cols = ['impr', 'clicks', 'cost', 'conversions', 'views', 'view_rate']
      df[float_cols] = df[float_cols].astype('float64')

      return df

def export_to_gbq(df):
  if 'ad_type' in df.columns:
    pandas_gbq.to_gbq(df, 'table_type', project_id='diuhub-amplifith-acquisition', if_exists='append')
  if 'age' in df.columns:
    pandas_gbq.to_gbq(df, 'table_age', project_id='diuhub-amplifith-acquisition', if_exists='append')
 
def etl_data(event, context):
  data, file_name = get_data(event, context)
  cleaned_data = clean_data(data, file_name=event['name'])
  export_to_gbq(cleaned_data)