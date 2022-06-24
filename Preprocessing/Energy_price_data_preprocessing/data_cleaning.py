'''

Module that does the cleaning part of the data processing for energy price
    model

This module does the following:
  6. Makes some basic cleaning (+ statistical analysis)
  7. Performs missing data analysis

'''

#!pip install pandas_market_calendars

import logging
import matplotlib.pyplot as plt
import pandas as pd
import pandas_market_calendars as mcal # To check if our commodities missing data is on non-trading days 
import missingno as msno # To visualise and analyse missing data

from data_functions import remove_features, add_log_line

logger = logging.getLogger(__name__)

def run_clean(df, features_info, show_plots):
    
    '''
        Main method that will call all functions to run the data cleaning
        Inputs:
            - df: Our merged dataframe containing all our data
            - features_info: Dictionary with features info
            - show_plots: If True, it will show some data plots
    '''

    add_log_line()
    logger.info('Data cleaning')
    add_log_line()

    # We do some cleaning purely based on % of available data
    features_to_remove = analyze_missing_data_features(df, show_plots)
    
    df = remove_features(df, features_to_remove, show_plots)
       
    logger.info('Missing data stats after removing features:')
    find_valid_entries(df, 'after', show_plots)
    
    if show_plots:
        commodities_yahoo_eda(df, features_info)

    return df


def commodities_yahoo_eda(df, features_info):
    
    '''
        Exploring missing values: For commodities and Yahoo ETFs, we remove 
        weekends / holidays, and check missing data. This is only a section 
        to prove that we have the data we need, and that the missing values 
        are only due to weekends/holidays
        NO DATA MODIFICATION DONE IN THIS SECTION
    '''
    
    def fetch_trade_days(df, market_ref = 'NYSE'):
        '''
        We fetch the dates where trading was open
        Inputs: 
            - Our data df
            - Market ref: Market reference we want to consider. Some example:
                - NYSE: New York Stock Exchange. This is the default since is 
                    where we found less missing data
                - OSE: Oslo Stock Exchange 
                - EUREX
                - LSE: London Stock Exchange
                
                
        Returns: List with trading dates
        '''
        
        trading_days = pd.DataFrame(
            {"date": mcal.get_calendar(market_ref).valid_days(
                start_date=df['datetime'].iat[0], 
                end_date=df['datetime'].iat[-1])})
        
        trading_days['date'] = trading_days['date'].dt.tz_convert(None)
        return trading_days['date'].values
    
    # We create a df with only commodities/ETFs
    df_subset = (df[['datetime'] + features_info['Commodities'] + 
                    features_info['Yahoo ETFs']]) 
    
    # We fetch our trading days
    trading_days = fetch_trade_days(df) 
    
    df_dates = pd.DataFrame(data=pd.to_datetime(df_subset['datetime']), 
                            columns=['datetime'])
    df_dates['Market day'] = [(ele in trading_days) for ele in df_dates['datetime']]
    
    logger.info('Missing data bar-plot for all commodities and Yahoo ETFs ' +
                 'considering all data')
    msno.bar(df_subset)
    
    logger.info('Shape before removing holidays is: ' + str(df_subset.shape))
    df_subset_wo_holidays = df_subset.loc[(df_dates['Market day'])]
    logger.info('Shape after removing holidays is: ' + 
                str(df_subset_wo_holidays.shape))
    
    
    logger.info('Missing data bar-plot for all commodities and Yahoo ETFs ' +
                 'excluding NYSE holidays/weekends')
    logger.info('Below plot is to show that except for Uranium, we do have' +
                ' all our expected data, and we are only missing mostly ' +
                 'holidays/weekends')
    
    msno.bar(df_subset_wo_holidays)
    

def plot_histo_box(x, show_plots, plot_text, status='before', bins=20, 
                   track = 'feature', plot = 'histo'):
    '''
    We plot either boxplot or histogram

    Inputs:
        x: Our data in either list or pd.Series format
        status: label that will be included in our plot to denote if our
            plot is before or after cleaning
        bins: Number of bins for histograms
        track: Defines which parameters are we tracking, either features
            or dates.

    '''
    if show_plots:
        if track == 'feature':
            label = ('% of missing dates for each feature ' + 
                status + ' cleaning')
        else:
            label = ('Missing number of features for each date ' + 
                status + ' cleaning')
    
        fig, ax = plt.subplots(figsize=(10, 6))
    
        if plot == 'histo':
            ax.hist(x, bins=bins)
            ax.set_ylabel('Occurances')
        else:
            ax.boxplot(x, vert=False)
            ax.set_ylabel('Boxplot')
    
        ax.set_xlabel(plot_text)
        ax.set_title(label)
        plt.show()
        # plt.text(0.5, 0.7, plot_text)
        
def find_valid_entries(df, status, show_plots):
    '''
    For a given dataframe, we check how many rows (i.e., samples/dates)
    have data for all features.
    This is done for the assumption that each of our samples require to have
    data for each of our features in order to be used in our model

    We also plot the same info through boxplots/histograms

    Input: Our dataframe of interest
    Return: Nothing (only prints)

    '''

    # We compute numer of missing entries:
    missing_entries = (df.isna() | (df=='.')).any(axis=1).sum() 
    missing_entries_pct = 100*(missing_entries)/df.shape[0]

    logger.info('We found ' + str(missing_entries_pct) + '% of ' +
                 'samples/dates with missing data in some of the features.')


    # We now plot number of missing features for each sample:
    missing_entries = (df.isna() | (df=='.')).sum(axis=1)

    plot_text = ('Each data point is a different date, '+
                ' so plots tells us how many features are' +
                 ' we missing for each date.')
    
    logger.info('On average, we are missing ' + 
                  str(int(pd.Series(missing_entries).mean())) + 
                  ' features for each date!')
    
    plot_histo_box(missing_entries, show_plots, plot_text, status, 
                   track = 'date')
    plot_histo_box(missing_entries, show_plots, plot_text, status, 
                   track = 'date', plot = 'box')    
    
def analyze_missing_data_features(df, show_plots, threshold = 50):
    '''
    This method analyzes from a dataframe all the features (columns) that 
    either have no data at all, or have data in only threshold % of the data 
    (threshold being an input)
    
    It also plots histograms and boxplots of our missing data distributions,
    both from the point of view of missing dates for each feature, as well
    as from missing features for each date.
    
    Inputs:
        - df: Our original dataframe with all features
        - show_plots: If True, we will show some data plots
        - threshold: Threshold in pct upon which we want to remove a feature
            from our dataframe (i.e., if feature has threshold % of missing
                                data)
    
    Returns:
        - List of features that dont meet data threshold criteria
    '''
        
    # logger.info(df.shape)

    find_valid_entries(df, 'before', show_plots)

    n_entries = df.shape[0]
    
    no_miss_data = [] # List of features without any missing all
    fully_empty = [] # List of features that are fully empty
    th_empty = [] # List of features that have threshold % of missing data
    ok_data = [] # List of features with {0 < missing data < threshold %}
    missed_pct_list = [] # List of our 
    
    for col in df.columns:       
        
        if col != 'datetime':
            # For missing data, we consider entries with either nans, or entries with 
            # "." (based on observed way missing entries are)
            n_missed = df[col].isna().sum()
            n_missed += (df[col]=='.').sum()
            
            missed_pct = 100*(n_missed/n_entries)
            missed_pct_list.append(missed_pct)
            
            if missed_pct == 0:
                no_miss_data.append(col)
            elif missed_pct == 100:
                fully_empty.append(col)
            
            elif missed_pct >= threshold:
                th_empty.append(col)
            
            else:
                ok_data.append(col)

    plot_text = ('Each data point is a feature, so plots tells' +
                 ' us how many dates we miss for each feature')
    
    logger.info('On average, we are missing ' + 
          str(int(pd.Series(missed_pct_list).mean())) + 
          ' dates for each feature!')
    
    plot_histo_box(missed_pct_list, show_plots, plot_text, 'before')
    plot_histo_box(missed_pct_list, show_plots, plot_text, 'before', 
                   plot = 'box')

    logger.info('Found ' + str(len(no_miss_data)) + ' feature(s) with ' +
                 'data in all entries (so no missing data at all): ' + 
                 str(no_miss_data))

    logger.info('Found ' + str(len(fully_empty)) + ' feature(s) with '
          'no data at all (all nans or dots): ' + str(fully_empty))

    logger.info('Found ' + str(len(th_empty)) + ' feature(s) with '
          'more than ' + str(threshold) + '% of missing data (we will ' +
          'delete these): ' + str(th_empty))

    logger.info('Found ' + str(len(ok_data)) + ' feature(s) with some' +
                 ' missing data but below ' + str(threshold) + '%')
       
    series = pd.Series(missed_pct_list)
    series = series[series<threshold]
    
    plot_text = ('Each data point is a feature, so plots tells ' +
                 'us how many dates we miss for each feature')
    
    logger.info('On average, we are missing ' + str(int(series.mean())) + 
          ' dates for each feature!')
    
    plot_histo_box(series, show_plots, plot_text, 'after')
    plot_histo_box(series, show_plots, plot_text, 'after', plot = 'box')
    
    logger.info('Based on the criteria of removing features with more than ' + 
                str(threshold) + '% of missing data, we removed a total of ' + 
                str(len(th_empty) + len(fully_empty)) + ' feature(s) in our' + 
                ' merged dataframe')
    
    return th_empty+fully_empty