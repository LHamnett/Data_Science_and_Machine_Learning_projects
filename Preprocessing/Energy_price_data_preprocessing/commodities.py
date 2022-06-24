'''
This module handles the commodities raw data as follows: 
    1. Takes the xls files, removes unnecessary data, corrects format, and 
        stores files as csv
'''
import pandas as pd


def clean_and_save(datasource, dest_path):
    cols = ['Schluss','Eroeffnung','Tageshoch','Tagestief']
    dataname = ['Commodities_Brent_Oil','Commodities_Gas',
                'Commodities_Coal','Commodities_Uranium']
    cols_to_drop =  ['Eroeffnung','Tageshoch','Tagestief']

    for count in range(0,4):
    
        source_df = datasource[count].copy()
        # Changing type from object to float
        source_df[cols] = source_df[cols].apply(lambda x: pd.to_numeric(x.astype(str)
                                                   .str.replace(',','.'), errors='coerce'))
        # Dropping Opening, Max, Min prices, keeping Closing values only
        source_df=source_df.drop(cols_to_drop, axis=1)

        #Renaming columns
        source_df = source_df.rename(columns={"Schluss":"daily_USD","Datum":"Date"})
    
        #Saving as individual csv files
        source_df.to_csv(dest_path + dataname[count]+'.csv', index = False)
        

def commodities_main(source_path, dest_path):
    '''
     Input: 
         - source_path: Path where our source data is located
         - dest_path: Path where our desitnation path is located (where we 
                        want to store the data that is modified)
    
    Output:
        - Stores new files into new local folder
    '''
    
    df_Brent_finanzennet_path = source_path + "Brent_USD_finanzennet.xlsx"
    df_Gas_finanzennet_path = source_path + "Gas_USD_finanzennet.xlsx"
    df_Coal_finanzennet_path = source_path + "Coal_USD_finanzennet.xlsx"
    df_Uranium_finanzennet_path = source_path + "Uranium_USD_finanzennet.xlsx"

    dateparser = lambda x: pd.to_datetime(x, dayfirst=True)

    dataBrent = pd.read_excel(df_Brent_finanzennet_path, parse_dates=['Datum'], 
                              date_parser = dateparser)
    dataGas = pd.read_excel(df_Gas_finanzennet_path, parse_dates=['Datum'], 
                            date_parser = dateparser)
    dataCoal = pd.read_excel(df_Coal_finanzennet_path, parse_dates=['Datum'], 
                             date_parser = dateparser)
    dataUranium = pd.read_excel(df_Uranium_finanzennet_path, parse_dates=['Datum'], 
                                date_parser = dateparser)
    datasource = [dataBrent,dataGas,dataCoal, dataUranium]

    #clean and save the data
    clean_and_save(datasource, dest_path)

# commodities_main()


