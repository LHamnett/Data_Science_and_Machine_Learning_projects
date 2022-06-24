# -*- coding: utf-8 -*-
"""
This module contains general functions that are used across different
data modules
"""
import logging
import missingno as msno # To visualise and analyse missing data

logger = logging.getLogger(__name__)

def add_log_line():
    logger.info('#####################################################' +
                '#####################################################' +
                '#####################################################'
                )   
    
def remove_features(df, features_to_remove, show_plots):
    '''
    This method removes a list of given features, and does
    some plotting of both those features, as well as resulting
    dataframe after removing them
    
    Inputs:
        - df: Our original dataframe with all features
        - features_to_remove
        - show_plots: If True, we will plot msno bar
    
    Returns:
        - Our df with removed features
    '''
    
    def drop_by_list(df, my_list):
        '''
        We drop all columns of a df based on a list of column names
        
        Inputs:
            - df: Our input dataframe
            - my_list: List of columns we want to drop in our df
        
        Returns:
            - df with dropped columns
            
        '''
        for col in my_list:    
            df.drop(columns=[col], inplace=True)
    
        return df
    
    if show_plots:
        logger.info('Missing data plot of the ' + 
                    str(len(features_to_remove)) + 
                    ' features WE WILL exclude in our merged dataframe:')
        
        msno.bar(df[(['datetime'] + features_to_remove)])
    
    # We remove the features:
    df = drop_by_list(df, features_to_remove)
    
    return df

def store_merged_df(dest_path, source, df):
    df.to_csv(dest_path + source + '_data_merged.csv', 
                     sep=',', na_rep='.', index=False)
    
    if source != 'Features':
        logger.info('Dataframe for data source ' + source + 
                     ' merged and stored locally as csv!')
    else:
        logger.info('Dataframe with all features merged and stored locally' +
                     ' as csv!') 
        