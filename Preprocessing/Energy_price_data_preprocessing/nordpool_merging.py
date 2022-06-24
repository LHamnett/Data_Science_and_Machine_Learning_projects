'''
    This module aggregates the Nordpool data. 
    It will look at all the .xls files in the raw folder and then aggregate 
    them into a single csv and resave the csv. This means:
        Input: raw_folder, with each file being a feature per year
        Output: For each feature, it merges all years into a combined one
                (one file per feature) and stores it locally

'''

import os
import pandas as pd
import logging


logger = logging.getLogger(__name__)

def nord_merge(source_path, dest_path):
    '''
    Inputs:
        source_path: Path where our raw data is located
        dest_path: Path where we want to store our merged data
    
    Returns:
        No return (but it stores all files locally)
    '''
    
    logger.info('For each Nordpool feature, we aggregate all the yearly ' + 
                'xls files into a single csv with all data combined. ' + 
                'This is quite time-consuming, so please have patience ;)')
    
    skipped = []
    for root, dirs, files in os.walk(source_path, topdown=True):
        root = root.replace('\\','/')
        root = root.replace('//','/')
        if 'unused_data' not in root and 'archive' not in root:
           for folder in dirs:
                logger.info('Scanning folder ' + str(join(root,folder)))
               
                if folder not in ['hydro_reservoir', 'archive', 'unused_data']:
                     c_files = os.listdir(join(root,folder)) # Current files
                    
                     if sum([ele[-3:] == 'xls' for ele in c_files])>1:
                             # As long as we have more than 1 xls, we 
                             # process it
                             create_concat(join(root,folder),dest_path)    
                     else:
                         pass
                elif folder == 'hydro_reservoir':
                     handle_hydro(join(root,folder),dest_path) 
                else:
                    skipped.append(str(folder))
                    # logger.info('Skipping folder ' + str(folder))   
        else:
            skipped.append(str(root.split('/')[-1]))
            # logger.info('Skipping folder ' + str(root.split('/')[-1]))
                   
    if len(skipped):
        logger.info('We skipped the following folders: ' + str(skipped))
    else:
        logger.info('No folders were skipped')
        
    logger.info('Finished Nordpool yearly aggregation')
    logger.info('----------------------------------------------------------')

def join(path1, path2):
    '''
    We join two paths
    '''
    
    if path1[-1] == '/':
        return path1 + path2
    else:
        return path1 + '/' + path2
    
def handle_hydro(h_dir, dest_path):    
    '''
    We handle hydro data:
    Hydro data is also in weird format and cannot be handled automatically 
    and also has nly weekly data. I had to generate the csv files manually 
    and then combine them. 
    
    Inputs:
        - h_dir: Directory of our raw hydro data
        - dest_path: Directory where we want to store combined hydro-data
    '''
    
    #need to handle hydro data differently as it fails on automated aggregation
    #created csv files manually from hydro xls files
    hydro_df = []
    # h_dir = '/home/leon/Documents/think_outside/Omdena-think_outside_repo/NordPool_data/hydro_reservoir'
    for file in os.listdir(h_dir):
        # logger.info(str(file))
        if file[-3:] == 'csv':
            temp_df = pd.read_csv(join(h_dir,file),header=1)
            temp_df['week'] = temp_df['Unnamed: 0'].apply(lambda x: x.strip(' ').split('-')[0]) #get week and year
            temp_df['year'] = temp_df['Unnamed: 0'].apply(lambda x: x.strip(' ').split('-')[1])
            temp_df = temp_df[['week', 'year','NO', 'SE', 'FI']]
            hydro_df.append(temp_df)
    
    hydro_concat = pd.concat(hydro_df)
    hydro_concat = hydro_concat.sort_values(by = ['year', 'week'], ascending = [True, True]).reset_index(drop=True)
    hydro_concat.head()
    
    filename = 'concat_hydro_reservoir_weekly.csv'
    store_df_to_csv(hydro_concat, join(dest_path, filename), filename)
    hydro_concat.to_csv()

def read_file(input_file):
    '''
        Reads xls file into df
    '''

    temp_df = pd.read_html(input_file,decimal=',',thousands='') #returns list of dataframes
    temp_df = temp_df[0] #select df of interest
    x = 0
    for x in range(10):# we try to get the level values from the multi-index until an error is thrown, then we use x-1 to get the data
        try:
            temp_df.columns.get_level_values(x)
        except:
            break
    
    temp_df.columns = temp_df.columns.get_level_values(x-1)
    
    temp_df = temp_df.rename(columns={temp_df.columns[0]: "date"}) #name date column
    
    if ('Hours' in temp_df.columns.values) == True: #if hours include hours in datetime else just use date
        temp_df['datetime'] = temp_df['date'] + ' ' + temp_df['Hours'].apply(lambda x: x[0:2]) +':00'
        temp_df = temp_df.drop(columns=['date','Hours'])
        temp_df['datetime'] = pd.to_datetime(temp_df['datetime'].values,format='%d-%m-%Y %H:%M') #convert to datetime
        
    else: 
        temp_df['datetime'] = temp_df['date'] 
        temp_df = temp_df.drop(columns=['date'])
        temp_df['datetime'] = pd.to_datetime(temp_df['datetime'].values,format='%d-%m-%Y') #convert to datetime
    
    temp_df  = temp_df.set_index('datetime')
#     temp_df = temp_df.sort_index(ascending=True) #sort from earliest to latest , handled during concat for efficiency
    
    return temp_df


def read_file_elbas_vol(input_file):
    '''
        We need a different function to read the elbas_volume_daily files 
        as they are very inconsistent in both format and numberof columns.
    '''
    temp_df = pd.read_html(input_file,decimal=',',thousands='') #returns list of dataframes
    temp_df = temp_df[0] #select df of interest
    x = 0
    for x in range(10):# we try to get the level values from the multi-index until an error is thrown, then we use x-1 to get the data
        try:
            temp_cols = temp_df.columns.get_level_values(x)
            if temp_cols[1] == 'NO1': #we look through the multi-index until we get to the level with NO1 column names
                break
            else:
                pass
        except:
            break
    
    temp_df.columns = temp_df.columns.get_level_values(x)
    new_cols = temp_df.columns.values
    
    #we want to add back in the buy and sell information to the column names which was lost earlier
    
    for col in range(len(new_cols)):#loop through column names
        if new_cols[col] == 'datetime': #ignore datetime columns
            pass
        if col % 2 == 0:
            new_cols[col] = str(temp_df.columns.values[col]) + '_sell'
        else:
            new_cols[col] = str(temp_df.columns.values[col]) + '_buy'
            

    temp_df = temp_df.rename(columns={temp_df.columns[0]: "date"}) #name date column
    
    #update column names
    for col in range(len(new_cols)): 
        if col == 0:
            pass
        else:
            temp_df = temp_df.rename(columns={temp_df.columns[col]: new_cols[col]})
    

    if ('Hours' in temp_df.columns.values) == True: #if hours include hours in datetime else just use date
        temp_df['datetime'] = temp_df['date'] + ' ' + temp_df['Hours'].apply(lambda x: x[0:2]) +':00'
        temp_df = temp_df.drop(columns=['date','Hours'])
        temp_df['datetime'] = pd.to_datetime(temp_df['datetime'].values,format='%d-%m-%Y %H:%M')
        
    else: 
        temp_df['datetime'] = temp_df['date'] 
        temp_df = temp_df.drop(columns=['date'])
        temp_df['datetime'] = pd.to_datetime(temp_df['datetime'].values,format='%d-%m-%Y')
    
    
    temp_df  = temp_df.set_index('datetime')
    temp_df = temp_df.sort_index(ascending=True) #sort from earliest to latest , handled during concat for efficiency
    
    return temp_df


def concat_files_folder(folder):
    '''
        Will concat all xls files in a folder into pandas dataframe
    '''
    files = os.listdir(folder) #get list of files in folder
    df_list = []
    files_list = []
    for file_number in range(len(files)): #loop through files
        if files[file_number][-3:] == 'xls': #only read xls files
            files_list.append(str(files[file_number]))
            # logger.info('Reading ' + str(files[file_number]))
            #use different function for elbas volume daily
            if folder.split('/')[-1] == 'elbas_volume_daily': #check if folder is troublesome folder
                try: 
                    # logger.info('alternate function selected')
                    df = read_file_elbas_vol(join(folder,files[file_number]))
                except:
                    # logger.info('reverting to normal function')
                    df = read_file(join(folder,files[file_number]))
            else:
                df = read_file(join(folder,files[file_number])) #read file from path
            # logger.info(str(files[file_number]) + ' read ok')
            df_list.append(df)
        else:
            pass
    
    if len(files_list) > 0:
        logger.info('Merged the following files together: ' + str(files_list))
    try:
        concat_df = pd.concat(df_list)
    except:
        logger.warning('Could not run concat of our dataframes. Should this' +
                       ' folder consider read_file_elbas_vol ' +
                       'function instead?')
        concat_df = pd.concat(df_list)
        
    concat_df = concat_df.sort_index(ascending=True)
    
    return concat_df


def create_concat(input_folder,dest_path):
    '''
        Runs through files, makes pandas df and then saves as csv into same 
        folder
    '''
    
    # Look at working directory and get current folder
    current_folder = input_folder.split('/')[-1] 
    
    concat_df = concat_files_folder(input_folder) #concat all xls files within folder
    newfilename = 'concat_' + current_folder + '.csv'#generate new filename
    csv_path = join(dest_path,newfilename)
    store_df_to_csv(concat_df, csv_path, newfilename)

    return concat_df

def store_df_to_csv(df, csv_path, filename):
    '''
    We store our df to a csv
    '''
    if os.path.isfile(csv_path):
        logger.info(str(filename) + ' created successfully by ' + 
                        'over-writing previous existing one')
    else:
        logger.info(str(filename) + ' created successfully by creating' +
                    ' new file (file did not exist previously)')
    
    df.to_csv(csv_path) #save as csv in specific folder
    
