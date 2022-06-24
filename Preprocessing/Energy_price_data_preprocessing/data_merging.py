'''

Module that does the merging part of the data processing for energy price model

This module does the following:
  1. Merges all the Nordpool yearly aggregated csvs into a single dataframe /
      csv file (and fixes inconsistencies).
  2. Merges all the weather csvs into a single dataframe/csv file.
  3. Merges all the Yahoo Finance ETFs csvs into a single dataframe/csv file
  4. Merges all the Commodities (Oil, Gas, Uranium, Coal) prices csvs into a 
      single dataframe/csv file (and cleans/formats data first).
  5. Merges all above Nordpool + weather + Yahoo Finance + Commodities prices 
      into a single features dataframe/csv file.
      
To control the setup in how to merge, and where is all data location placed/stored at,
see merge_setup method below.

'''
import logging
import pandas as pd
import numpy as np
import os
from data_functions import store_merged_df, add_log_line
from nordpool_merging import nord_merge
from commodities import commodities_main

logger = logging.getLogger(__name__)

def merge_setup():
    '''
    We define the merging setup we want to consider
    
    When adding new data sources, please UPDATE data_sources, source_paths, 
        destination_paths
    
    Returns: 
        - data_sources: List containing which data sources we want to merge 
            into single csv.  
         
        - source_paths: Dictionary containing our relative data source path 
            location for each data source
        
        - destination_paths: Dictionary containing the relative destination
            path for the merged dfs. Also includes the destination path for 
            Features, where a dataframe merged from ALL data is placed.
        
        - overwrite_dates: Dictionary defining the start and end dates we want
            to consider for our final merged dataframe.
            This will remove any dates that are out of that range once 
            dataframe is merged. On top of this, the code also limits the 
            start/end to be the most constraint dates of all our data-sources.
            (so overwrite_dates will only remove any extra data beyond those 
            data-sources limits, it will not create or enforce new dates)
        
        - refetch_nordpool_raw: 
                If True, it will re-fetch the Nordpool data from raw-folder, 
                    and aggregate all raw files into combined yearly file. 
                
                If False, it will fetch Nordpool data directly from 
                    the clean data path
                    
    '''
    data_sources = ['Nordpool', 'Weather', 'Elspot_GloH2OPast','Yahoo ETFs',
                    'Commodities'] 
    
    overwrite_dates = {'start': pd.Timestamp('2014-01-01'), 
                       'end': pd.Timestamp.now()}
    
    refetch_nordpool_raw = True
    
    data_path = '../../data/'
    raw_data_path = data_path + 'raw_data/'
    clean_data_path = data_path + 'cleaned_data/'
    processed_data_path = data_path +  'processed_data/energy/'
    
    source_paths = {'Nordpool': raw_data_path + 'NordPool_data/',
                    'Weather': raw_data_path + 'task_3_weather_data/',
                    'Elspot_GloH2OPast': raw_data_path + 'elspot_GloH2O_data/Past/',
                    'catchment_GloH2O_dataPast': raw_data_path + 'catchment_GloH2O_data/Past/',
                    'Yahoo ETFs': raw_data_path + 'Yahoo_ETFs_data/',
                    'Commodities': raw_data_path + 'Commodities_data/'}
    
    destination_paths = {'Nordpool': clean_data_path + 'Nordpool_aggregated_csvs/',
                         'Weather': clean_data_path + 'task_3_weather_data_aggregated_csvs/',
                         'Elspot_GloH2OPast': clean_data_path + 'Elspot_GloH2O_data/Past',
                         'catchment_GloH2O_dataPast': clean_data_path + 'catchment_GloH2O_data/Past/',
                         'Yahoo ETFs': clean_data_path + 'Yahoo_ETFs_data_aggregated_csvs/',
                         'Commodities': clean_data_path + 'Commodities_data_aggregated_csvs/',
                         'Features': processed_data_path} # Features is our final df with all our features combined
    
    return (data_sources, source_paths, destination_paths, overwrite_dates,
            refetch_nordpool_raw)

def get_csv_file_names(source_path, verbose = True):
    '''
    We fetch the file names of our csv.
    Inputs:
        - source_path: Relative path where our csv data is located
        - verbose: If True, we will print all found csv file names
    Returns:
        - daily_csvs, hourly_csvs and all_csvs, which contain our csv file 
            names for daily/hourly/all
    '''
    #get daily csvs
    daily_csvs = []
    hourly_csvs = []
    all_csvs = []
    
    for file in os.listdir(source_path):
        if file[0] != '.' and 'csv' in str(file):
            all_csvs.append(file)
            if 'daily' in str(file):
                daily_csvs.append(file)
            elif 'hourly' in str(file):
                hourly_csvs.append(file)
    
    if 'concat_elspot_prices_no_daily.csv' in daily_csvs:
        # We want the euro prices instead of norwegian kroner prices
        daily_csvs.remove('concat_elspot_prices_no_daily.csv') 
    
    if verbose:
        logger.info('Found ' + str(len(all_csvs)) + ' all csvs:')
        logger.info(all_csvs)
        
        if len(daily_csvs) > 0:
            logger.info('Found ' + str(len(daily_csvs)) + ' daily csvs:')
            logger.info(daily_csvs)

        if len(hourly_csvs) > 0:
            logger.info('Found ' + str(len(hourly_csvs)) + ' hourly csvs:')
            logger.info(hourly_csvs)

    return all_csvs, daily_csvs, hourly_csvs

def adjust_dates(df, dates, overwrite_dates, date_label):
    '''
    This method adjust the dates of our dataframe based on two conditions:
    
        1. For start date, we adjust dates to the highest found start date in
            all our data-sources
        2. For end date, we adjust dates to the lowest found start date in 
            all our data-sources
        3. After doing above, we further remove start/end dates based on 
            user-defined overwrite_dates
    
    Inputs:
        - df: Our merged dataframe
        - dates: Dictionary with the start/end dates of all our data-sources
        - overwrite_dates: User-defined start/end dates that we want to enforce
        - date_label: The name of our date column
        
    Returns:
        - df: Our merged dataframe with adjusted dates
    '''
    
    def log_df(label, df):
        start = min(df[date_label])
        end = max(df[date_label])
        logger.info('' + label + ', our dataframe shape is: ' + 
                     str(df.shape) + ' with range dates of ' +
                     str(start) + ' - ' + str(end))
    
    logger.info('------------------------')
    logger.info('We adjust dates: ')
    log_df('Before adjusting dates', df)
    
    # 1. We limit start date:
    logger.info('The found starting dates for all our data-sources are: ' + 
          str(dates['start']))
    start = max(dates['start'].values())
    logger.info('Based on found start dates, we adjust our start date ' +
                 'to: ' + str(start))
    df = df[df[date_label] >= start]
    
    log_df('After adjusting start dates', df)
    
    # 2. We limit end date:
    logger.info('The found ending dates for all our data-sources are: ' + 
          str(dates['end']))
    end = min(dates['end'].values())
    logger.info('Based on found end dates, we adjust our end date to: ' + 
          str(end))
    df = df[df[date_label] <= end]
    
    log_df('After adjusting end dates', df)
    
    # 3. We adjust dates by user-defined limits:
    start = overwrite_dates['start']
    logger.info('User chose to limit start dates to: ' + str(start))
    df = df[df[date_label] >= start]
    log_df('After adjusting user-start dates', df)
    
    end = overwrite_dates['end']
    logger.info('User chose to limit end dates to: ' + str(end))
    df = df[df[date_label] <= end]
    log_df('After adjusting user-end dates', df)
    
    return df

def resample_hourly(source_path, dest_path, skip_csv=[
                                    'concat_elspot_prices_no_hourly.csv', 
                                    'concat_elspot_volumes_hourly.csv', 
                                    'concat_production_no_hourly.csv',
                                    'concat_regulating_prices_hourly.csv',
                                    'concat_consumption_no_hourly.csv'], 
                        agg_type='mean', agg_type_overwrite = {}):
    '''
        Method that re-samples hourly to daily csv. That is because for some 
            data we only have hourly, so we need to resample it to daily csvs.
            
        Inputs:
            - source_path: Relative path where our csv data is located
            - dest_path: Relative path where we want to store our resampled data
            - daily_csvs, hourly_csvs: File names of our daily/hourly csv files
            
            - skip_csv: List with the hourly file-name csvs we want to skip 
                        (meaning skipping conversion to daily). This is because 
                        we either already have raw daily data, or we don't 
                        need it.

            - agg_type: Defines how we want to convert hourly to daily. 
                        Options are: 'mean', 'min', 'max', 'first_entry', 
                        'last_entry'

            - agg_type_overwrite = {} Dictionary with file names where we want 
                to over-write agg_type to another specific aggregation type.
                Example: {'concat_production_no_hourly.csv': 'mean'} would 
                over-write production aggregation to 'mean', regardless of 
                agg_type.
        
        Returns: Nothing, but it stores all new daily csvs into data folder
        
    '''

    def input_consistency():
        # We check for input consistency:
        types = ['mean', 'min', 'max', 'first_entry', 'last_entry', 'mean']
        if agg_type not in types:
            logger.info('Chosen agg_type ' + str(agg_type) + 
                  ' is not currently supported! Please change it and ' + 
                  're-run code :)')

        for ele in agg_type_overwrite:
            if agg_type_overwrite[ele] not in types:
                logger.info('Chosen agg_type_overwrite ' + 
                      str(agg_type_overwrite[ele]) + 
                      ' for csv ' + str(ele) + ' is not currently ' + 
                      'supported! Please change it and re-run code :)')
 
    def check_type(file_agg_type):
        '''
        We check if current file matches required aggregation type
        Return True if it does, false if it doesn't.
        '''
        
        if ((file in agg_type_overwrite and agg_type_overwrite[file]== file_agg_type) or 
                (file not in agg_type_overwrite and agg_type == file_agg_type)):
            return True
        else:
            return False
    
    all_csvs, daily_csvs, hourly_csvs = get_csv_file_names(source_path)
    
    input_consistency()
    
    for file in hourly_csvs:
        
        if file not in skip_csv and file.replace('hourly', 'daily') in daily_csvs:
            logger.info('We skipped hourly file ' + file + ' since there ' +
                         'is already a non-aggregated daily csv in the ' +
                         'folder!')
            skip_csv.append(file)
        
        elif file not in skip_csv:
            temp_df = pd.read_csv(source_path + file, index_col = 'datetime', 
                                  parse_dates = True)
            if ((file in agg_type_overwrite and agg_type_overwrite[file]=='mean') or 
                (file not in agg_type_overwrite and agg_type == 'mean')):
            
                temp_df = temp_df.resample('D').mean()
            
            elif check_type('max'):
            
                temp_df = temp_df.resample('D').max()
            
            elif check_type('min'):
                temp_df = temp_df.resample('D').min()
            
            elif check_type('first_entry'):
                temp_df = temp_df.resample('D').first()
            
            elif check_type('last_entry'):
                temp_df = temp_df.resample('D').last()
                
            temp_df.to_csv(dest_path + file.replace('hourly', 'agg_daily'), 
                           sep=',', na_rep='.')
    
    converted_files = set(hourly_csvs)-set(skip_csv)
    if len(converted_files) > 0:
        logger.info('We converted the following ' + 
                     str(len(converted_files)) + ' files from hourly csvs ' +
                     'to daily csvs:')
        logger.info(converted_files)
    else:
        logger.info('We did not find any hourly csv file to convert!!')
        

def combine_dfs(concat_df_list, date_label, data_sources=[]):
    '''
    Given a list of dfs, we combine them all
    Inputs:
        - concat_df_list: List of dfs
        - date_label: Label of our datetime column
        - data_sources: List of our used data-sources
    
    Returns:
        - merge_df
        - features_info, which is a dictionary of {data-source: ['features']}
    '''
    #merging dfs    
    merged_df = pd.DataFrame()
    features_info = {}
    for df_iter in range(len(concat_df_list)):
        # Below if is the case of first pass and having only one element 
        # in the list
        if df_iter == 0 and len(concat_df_list)==1: 
            merged_df = concat_df_list[0]
        elif df_iter == 0: # nothing to merge on first pass as only 1 df present
            pass
        elif df_iter == 1: # combine first two dfs
            merged_df = concat_df_list[df_iter-1].merge(concat_df_list[df_iter], how='outer', 
                                       on=date_label,  
                                      copy=True, indicator=False, validate=None)
        else: # combine rest dfs
            merged_df = merged_df.merge(concat_df_list[df_iter], how='outer', 
                                       on=date_label,  
                                       copy=True, indicator=False, validate=None)
        
        if data_sources:
            # combine_dfs method is used first to combinate all features of 
            # a data-source, and also used to combine all data-sources. 
            # Below line is used only for 2nd option
            features_info[data_sources[df_iter]] = list(concat_df_list[df_iter].columns.values[1:])
        
    if len(concat_df_list):
        return merged_df, features_info
    else:
        logger.info('Merged df could not be generated! ')
        return pd.DataFrame(), {}
    
def merge_df(source_path, source, date_label, 
             skip_csv = [
                 # We skip below because is missing dates > 2020 (we use MSWX
                 # instead)
                 'GloH2O_precipitation_mswep_v280_daily.csv',
                 # We skip below since that is our old merged dataframe, which
                 # we want to avoid re-fetching it!
                 'Commodities_data_merged.csv'] 
                 # We skip this because elbas intra-day volume has too much 
                 # missing date (we have elspot day-ahead volumes anyway)
                 # 'concat_elbas_volume_daily.csv', 
            ):
    
    '''
    Returns a merged dataframe. 
    Inputs:
        - date_label: Name of our datetime column
        - source_path: Path where our source files are located
        - source: Data source used to merge
        - skip_csv: List with the csvs we want to skip 
    '''

    def consistency_check(df):
        '''
        For Nordpool raw data, we found some csv with inconsistent data.
        More specifically, some bidding zones were duplicated, 
        and data scattered in both duplicates. An example can be
        found in raw file elspot-prices_2020_daily_eur.xls
        
        This method look for those issues and fixes them (merges data)
        '''
        
        if source == 'Nordpool':
            changes = []
            for ele in df.columns:
                if '.1' in ele and ele[:-2] in df.columns:
                    changes.append(ele[:-2])

            if len(changes):
                for ele in changes:
                    df = df.assign(**{
                        ele: df[ele].fillna(df[ele+'.1'])})
                    df.drop([ele+'.1'], axis=1, inplace=True)
                
                logger.info('For Nordpool file ' + file + ', we found the ' +
                      'following features with ' + 
                      ' duplicated columns (extra columns with .1), ' + 
                      'so we merged them: ' + str(changes))
        
        return df
        
    # We fetch again our csv names since we changed them when aggregating hourly
    all_csvs, daily_csvs, hourly_csvs = get_csv_file_names(source_path)
    
    if source == 'Nordpool':
        ref_csv = daily_csvs
        logger.info('Using only daily csvs to merge ' + source + ' data.')
    else:
        ref_csv = all_csvs
        logger.info('Using all found csvs to merge ' + source + ' data.')
        
    concat_df_list = []
    suffix_list = []
    
    #create column suffixes from filename and create list of dfs
    for file in ref_csv:
        if file not in skip_csv:
            suffix = file
            suffix = suffix.split('.')[0] #remove .csv
            suffix1 = suffix.replace(" ","_")      
            words = suffix1.split('_')
            print

            if words[0] == 'concat': #remove 'concat' at elem 0
                words = words[1:]

            for word in ['daily', 'agg', 'no']:
                if words[-1] == word: # Remove word from last position
                    words = words[:-1]

            words = "_".join(words) #rejoin list into single string
        #     logger.info(words)
            suffix_list.append('_'+words)

            #dateparser = lambda x: pd.to_datetime(x, dayfirst=True)
            temp_df = pd.read_csv(source_path + file, parse_dates=True)
            #temp_df.sort_index(inplace=True)

            if temp_df.columns[0]=='Unnamed: 0' and temp_df.iat[0,0] == 0:
                logger.info('For df ' + file + ' we found unnamed first ' +
                             'column. We will ignore (delete) it.')
                temp_df.drop(temp_df.columns[0], axis=1, inplace=True)

            temp_df = consistency_check(temp_df)

            temp_df[temp_df.columns[0]]=pd.to_datetime(temp_df[temp_df.columns[0]])

            concat_df_list.append(temp_df)
    
    #add suffixes to df:
    for i in range(len(concat_df_list)): #loop through list of dfs
        temp_cols = concat_df_list[i].columns#.str.replace(' ','_') #get column names & remove spaces in names
      
        new_names = dict() #create empty dict to store old and new colum names
        for x in range(len(temp_cols)): # loop over column names
            if x == 0: 
                new_names[temp_cols[x]] = date_label # We enforce date label
            else:
                new_names[temp_cols[x]] = temp_cols[x].replace(' ','_')+suffix_list[i] #add to dict pair to map from old to new name
            x=x+1

    #    logger.info('new column names', new_names)
    #   once we have new column names, we apply to each df
        concat_df_list[i] = concat_df_list[i].rename(columns=new_names)

    
    merged_df, _ = combine_dfs(concat_df_list, date_label)
    # if source == 'Elspot_GloH2OPast':
    #     logger.info(merged_df)
    
    
    
    if source == 'Nordpool':
        # For Nordpool, the raw source stored missing data as ".", so we 
        # replace those to NaNs
        merged_df.replace(".", np.nan, inplace = True)
    
    return merged_df            

def inspect_df(source, df, date_label, dates, verbose = False):
    '''
    We inspect the dataframe
    '''
    pd.set_option('display.max_columns', 500)
    df.head()
    logger.info('' + source + ' merged dataframe shape:')
    logger.info(df.shape)
    
    if df.shape[0]>1 and date_label in df.columns:
        start = min(df[date_label])
        end = max(df[date_label])
        
        dates['start'][source] = start
        dates['end'][source] = end
        
        # logger.info('Dates from ' + str(start) + ' to ' + str(end))
        
    if verbose:
        logger.info('' + source + ' merged dataframe columns:')
        for x in df.columns:
            logger.info(x)
    
    return dates

def simplify_yahoo(df, date_label):
    '''
        For Yahoo data source, we only care about our date and Adj Close 
        data, so we delete any column that are not those (so we remove Open-
        High-Close-Low-Volume)
        Input: df. Output: Same df with filtered columns

    '''
    for col in df.columns:
        if not (col == date_label or 'Adj_Close' in col):
            df.drop(columns=[col], inplace=True)
    return df

def run_merge():
    data_sources, source_paths, destination_paths, overwrite_dates, refetch_nordpool_raw = merge_setup()
    merged_dfs = []
    dates = {'start': {}, 'end': {}} # Dict that will contain for each data_source our found dates start/end
    date_label = 'datetime'
    
    add_log_line()
    logger.info('Merging of data: ')
    add_log_line()
    
    for source in data_sources:
        logger.info('----------------------------')
        logger.info('Merging ' + source + ' data source:')
        logger.info('----------------------------')
        
        source_path = source_paths[source]
        dest_path = destination_paths[source]
        
        if source == 'Nordpool':
            # For Nordpool, we do two extra steps:
            # 1. We aggregate all yearly xls files into a combined csv
            if refetch_nordpool_raw:
                nord_merge(source_path, dest_path)
            # Above method gets raw data and places it in dest_path, so
            # from this point on, for Nordpool, we assume as our source_data
            # our dest_path:
            source_path = dest_path
            # 2. We aggregate hourly to daily
            resample_hourly(source_path, dest_path)
            
        elif source == 'Commodities':
            commodities_main(source_path, dest_path)
            # Above method gets raw data and places it in dest_path, so
            # from this point on, for commodities, we assume as our source_data
            # our dest_path:
            source_path = dest_path
            
        merged_df = merge_df(source_path, source, date_label)
        
        if source == 'Yahoo ETFs':
            # For Yahoo, we remove unnecessary columns
            merged_df = simplify_yahoo(merged_df, date_label)
    
        dates = inspect_df(source, merged_df, date_label, dates)
        store_merged_df(dest_path, source, merged_df)
        merged_dfs.append(merged_df)
  
    
    # We now merge all data sources into a single dataframe / csv:
    df, features_info = combine_dfs(merged_dfs, date_label, data_sources)
    
    logger.info('------------------------')
    logger.info('Merging finished')
    logger.info('------------------------')
    
    df = adjust_dates(df, dates, overwrite_dates, date_label)

    logger.info('------------------------')    
    
    return df, features_info, destination_paths
