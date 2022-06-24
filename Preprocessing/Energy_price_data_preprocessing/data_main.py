'''

Module to merge/process data for energy price prediction models.

This module (connected to all other sub-modules) does the following:
    From data_merging module:
          1. Merges all the Nordpool yearly aggregated csvs into a single 
              dataframe / csv file (and fixes inconsistencies).
          2. Merges all the weather csvs into a single dataframe/csv file.
          3. Merges all the Yahoo Finance ETFs csvs into a single dataframe/csv
          4. Merges all the Commodities (Oil, Gas, Uranium, Coal) prices csvs
              into a single dataframe/csv file.
          5. Merges all above Nordpool + weather + Yahoo Finance + Commodities 
              prices into a single features dataframe/csv file.
    
    From data_cleaning module:          
        6. Makes some basic cleaning (+ statistical analysis)
        7. Performs missing data analysis.
    
    From data_imputing module:
        8. Performs data imputation
    
    From this module:
      9. Stores file into local csv
      10. Stores an additional pickle that contains features-info

'''

# Dependencies

#!pip install missingno

import logging
import pandas as pd
# import math # To check single-value nans
import pickle # To store our features_info

import missingno as msno # To visualise and analyse missing data
from data_merging import run_merge
from data_cleaning import run_clean
from data_imputing import run_impute
from data_functions import store_merged_df, add_log_line


def setup_log():
    '''
        Method to setup the log file
    '''
    logging.basicConfig(level=logging.INFO,
                format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                datefmt='%m-%d %H:%M',
                filename= 'data_price_processing.txt',
                filemode='w')
    
    
    console = logging.StreamHandler()
    console.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(levelname)-8s %(message)s')
    console.setFormatter(formatter)
    logging.getLogger('').addHandler(console)
    logger = logging.getLogger(__name__)
    
    return logger

def extra_eda(show_plots):
    '''
     Perform extra EDA: Exploring missing values
    '''
    
    def matrix_analysis(df,startdate,finishdate):
        '''
       Function to plot date-ordered matrix analysis of missing data for a
       given datetime period
       '''
        df1 = df.set_index('datetime')
        df1.index = pd.to_datetime(df1.index, utc=True)
        msno.matrix(df1[startdate :finishdate], freq='M', figsize=(15, 20))
    
    if show_plots:
        # https://towardsdatascience.com/visualizing-missing-values-in-python-is-shockingly-easy-56ed5bc2e7ea           
        
        # https://medium.datadriveninvestor.com/easy-way-of-finding-and-visualizing-missing-data-in-python-bf5e3f622dc5
        
        # df.shape
        
        # df.dtypes
        
        # list(df.columns)
        
        
        # pd.set_option('display.max_rows',None)
        # df.isna().sum().any()
        
        msno.bar(df)
        
            
        matrix_analysis(df,'2021-01-02','2022-01-02')
        
        msno.dendrogram(df)
       
def run(show_plots = False):
    '''
    This method is the one that executes all code
    Inputs:
        - show_plots: If True, we will display all data plots
    
    Returns:
        - Stores all data files, and returns the final data file as df
        
    '''
    
    logger = setup_log()
    
    add_log_line()
    logger.info('This file will log the progress of the data merging ' + 
                '+ cleaning + imputing')
    
    df, features_info, destination_paths = run_merge()    

    # df.sort_index(inplace=True) # We can't use index since datetime is not our index!
    df.sort_values(by='datetime', inplace=True)
    df = run_clean(df, features_info, show_plots)
    add_log_line()
    
    df = run_impute(df, show_plots)
    add_log_line()
    
    # We store our dataframe and our features_info
    store_merged_df(destination_paths['Features'], 'Features', df)
    
    with open(destination_paths['Features'] + 'features_info.pickle', 'wb') as handle:
        pickle.dump(features_info, handle, protocol=pickle.HIGHEST_PROTOCOL)
        
    logger.info('Features with all data-sources combined into single ' + 
                'dataframe and stored locally at path ' + 
                destination_paths['Features'])
    
    logger.info('Final dataframe shape: ' + str(df.shape))
    logger.info('Data processing finalized.')
    
    extra_eda(show_plots)
    
    logger.info('Final dataframe contains data dates from ' + 
                str(df['datetime'].iat[0]) + ' to ' + 
                str(df['datetime'].iat[-1]))
    
    return df

df = run()
