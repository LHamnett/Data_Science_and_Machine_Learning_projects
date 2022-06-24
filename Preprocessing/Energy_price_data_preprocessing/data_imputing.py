'''

Module that does the imputation part of the data processing for energy price
    model

'''

import pandas as pd
import numpy as np
from data_functions import remove_features, add_log_line

import logging

logger = logging.getLogger(__name__)

def run_impute(df, show_plots):
    
   ''' 
       We perform nan analysis and remove features that have too many 
       consecutive nans, and also impute the data 
   '''

   add_log_line()
   logger.info('Data imputation:')
   add_log_line()

   df = perform_nan_analysis(df, show_plots)
    
   df = perform_imputation_part1(df, show_plots)
    
   df = perform_imputation_part2(df)
    
   return df
    

def perform_nan_analysis(df, show_plots):
    
    '''
    This method performs analysis of consecutive nans. It does the following:
        - Data analysis (+ some plots)
        - Remove features that have too many consecutive nans
       
    Returns: Our df with removed features
    
    '''
    
    def find_consec_nans(df, nan_groups, nan_group_vals):
        '''
        
        This method returns a dataframe with the number of consecutive nans 
        found for each feature.
        The dataframe is splitted into different groups, each row being the 
        amount of instances
        with X consecutive nans.
        
        Input:
            - nan_groups and nan_group vals: Two lists where each element 
                defines each sub-group of consecutive nans. They are connected, 
                since the nan_groups are the labels, and the nan_group_vals 
                are the real values feed to do the math
        
        '''
        df_nans = pd.DataFrame(index=nan_groups)
        df_nans.index.name = 'Consec nans'

        for feature in df.columns:
            s = df[feature].isna().groupby(df[feature].notna().cumsum()).sum()
            s = s[s!=0]
            # We create different bins with different amount of consecutive nans
            b = pd.cut(s, bins=nan_group_vals, labels=nan_groups)
            out = b.groupby(b).size().reset_index(name='Cases')
            df_nans[feature]=out['Cases'].values
        
        return df_nans

    def plot_group_nans(nan_groups, df_nans, all_cols, n_features = 5):
        '''
        For each nan group, we find the features with > 0 nans, and show those 
        in the dataframe. We also sort that dataframe by number of found 
        instances
        
        Inputs:
            - n_features: Amount of features we want to display (this will
                        be the worst n_features found in the df)
        '''
        
        for nan_group in nan_groups:
            # We fetch the features that has more than nan_group consecutive nans
            nan_cols = all_cols[(df_nans.loc[nan_group]>0).values]
            # We remove Nordpool features
            # nan_cols = [ele for ele in nan_cols if ele not in features_info['Nordpool']] # Removes all Nordpool data
            # nan_cols = [ele for ele in nan_cols if 'transmission' not in ele] # Removes all Nordpool transmissions

            # We display those cases, sorted by amount of cases found
            logger.info('Showing ' + str(n_features) + ' out of ' +
                         str(len(nan_cols)) + ' features'  +
                         ' with ' + nan_group + ' consecutive nans, sorted' +
                         ' by # of found cases (the number of each feature ' +
                         'indicate the found instances with X consecutive' +
                         ' nans)')
            
            df_plot = df_nans[nan_cols].sort_values(by=nan_group, axis = 1, 
                                                       ascending = False)
            
            logger.info(df_plot.iloc[:,:5].to_string())
            
    
    def find_nan_features(nan_groups, all_cols):
        '''
        We remove the features that have too many consecutive nans
        Returns: Our list of removed features
        '''
        features_to_remove = []
        for nan_group in nan_groups:
            if nan_group in ['6-15', '16 and above']:
                if nan_group == '6-15':
                    nan_cols_remove = list(all_cols[(df_nans.loc[nan_group]>10).values])
                else:
                    nan_cols_remove = list(all_cols[(df_nans.loc[nan_group]>10).values])

                logger.info('Removing the following ' + str(len(nan_cols_remove)) + 
                      ' features due to too many ' + nan_group + 
                      ' consecutive nans: ' + str(nan_cols_remove))
                features_to_remove += nan_cols_remove
        
        features_to_remove = list(set(features_to_remove))
        logger.info('We will remove a total of ' + 
                     str(len(features_to_remove)) + 
                     ' features: ' + str(features_to_remove))
        
        return features_to_remove
    
    nan_groups = ['0-2','3-5','6-15', '16 and above']
    nan_group_vals = [0, 2, 5, 15, np.inf]
    
    all_cols = np.array(df.columns.to_list())
    df_nans = find_consec_nans(df, nan_groups, nan_group_vals)
    if show_plots:
        plot_group_nans(nan_groups, df_nans, all_cols)
    
    features_to_remove = find_nan_features(nan_groups, all_cols)
    
    df = remove_features(df, features_to_remove, show_plots)
    
    if show_plots:
        logger.info('We show same consecutive nan analysis after ' +
                    'removing nans')
        
        all_cols = np.array(df.columns.to_list())
        df_nans = find_consec_nans(df, nan_groups, nan_group_vals)
    
        plot_group_nans(nan_groups, df_nans, all_cols)
    
    return df


# ### We perform data imputation

def plot_nans(df, all_cols, label = 'ffill'):
    # We plot the features with still nans after ffill method:
    nan_cols = all_cols[(df.isna().sum()>0)]
    logger.info('After ' + label + ', we still have the following' +
                 ' features with NaNs:')
    logger.info(df[nan_cols].isna().sum().to_string())
        
def perform_imputation_part1(df, show_plots, nan_th = 100):
    '''
        This method performs data imputation, as follows:
            1. It uses ffillna with method ff
            2. After ffillna, it will remove any features that still have more 
            than nan_th nans
            3. It plots visually those discarded features
        
        There is another method that will finalize imputation (part2), we put 
            it in different methods since jupyter wrongly place the plots 
            otherwise!
        
        Returns:
            df with imputed data
    '''
    logger.info('We perform data imputation based on ffill')
    df.fillna(method = 'ffill', inplace = True)
    
    all_cols = np.array(df.columns.to_list())
    plot_nans(df, all_cols)
    
    # We now remove features with more than nan_th nans:
    features_to_remove = list(all_cols[(df.isna().sum()>=nan_th)])
    
    logger.info('We removed ' + str(len(features_to_remove)) + 
                 ' feature(s) that have more than ' + str(nan_th) + 
                 ' nans after ffill: ' + str(features_to_remove))
    
    df = remove_features(df, features_to_remove, show_plots)
   
    return df

def perform_imputation_part2(df):
    '''
        This method performs data imputation (part 2), as follows:
            1. For each feature with NaNs, it checks if NaNs are at beginning 
                of data. If they are, it counts how many days since start we 
                have NaNs, and remove those dates (since the only way to 
                impute those would be looking into the future, which we want 
                to avoid!)
                
            2. For remaining NaNs (which I think should be none), it will 
                replaces them with the mean of last 10 values
        
        Returns:
            df with imputed data
    '''
    
    all_cols = np.array(df.columns.to_list())
    plot_nans(df, all_cols, label = 'removing extra features')
    
    nan_features = list(all_cols[(df.isna().sum()>0)])
    
    if len(nan_features) > 0:
        # We check if NaNs are due to start of data (in which case we can't 
        # impute w/o looking into the future!)
        max_dates = 0
        for feat in nan_features:
            tmp_dates = 0
            for row in range(df.shape[0]):
                if pd.isna(df[feat].iat[row]):
                    tmp_dates = row
                else:
                    if tmp_dates > max_dates:
                        max_dates = tmp_dates
                    break
        
        if max_dates > 0:
            logger.info('We plot the first entries of our features ' +
                         'with nans:')
            logger.info(df[nan_features].head(max_dates*2).to_string())
            logger.info('Found at least one feature with first ' + 
                  str(max_dates+1) + ' days with missing data.' + 
                  ' We remove those ' + str(max_dates+1) + ' first ' + 
                 'dates to avoid looking into future to impute data!')
            df = df.iloc[(max_dates+1):, :]
            
                    
    nan_features = list(all_cols[(df.isna().sum()>0)])
    if len(nan_features) > 0:
        logger.info('We replace remaining NaNs with the mean of last 10 values')
        df_ma = df[nan_features].rolling(window=10, min_periods = 1).mean()
        for feat in nan_features:
            df[feat].fillna(df_ma[feat], inplace=True)
    
    nan_features = list(all_cols[(df.isna().sum()>0)])
    if len(nan_features) > 0:
        logger.warning('UNEXPECTED BEHAVIOR!!: After replacing NaNs with ' +
                     'mean, we found ' + str(len(nan_features)) + 
                     ' feature(s) with NaNs: ' + str(nan_features))
        logger.warning('Above nans will stay in our df!')
    else:
        logger.info('No found NaNs after performing imputation :)')
        
    return df