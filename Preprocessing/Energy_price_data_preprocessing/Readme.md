### Energy-price data-handling flow:

This document explains how is the data-handling code structured for the specific case of energy price-prediction.

The main python module is data_main.py (and only that one has to be run), which then calls all other python modules within the same folder.

data_main.py (connected to all other modules) will do the following:

	From data_merging module:
		1. Merges all the Nordpool yearly aggregated csvs into a single 
		  dataframe / csv file (and fixes inconsistencies).
		2. Merges all the weather csvs into a single dataframe/csv file.
		3. Merges all the Yahoo Finance ETFs csvs into a single dataframe/csv
		4. Merges all the Commodities (Oil, Gas, Uranium, Coal) prices
		  into a single dataframe/csv file.
		5. Merges all above Nordpool + weather + Yahoo Finance + Commodities 
		  prices into a single features dataframe/csv file.

    From data_cleaning module:          
		6. Makes some basic cleaning (+ statistical analysis)
		7. Performs missing data analysis.

    From data_imputing module:
        8. Performs data imputation
    
    From data_main.py module:
		9. Sorts dataframe based on dates (to enforce sequential order).
		10. Stores file into local csv
		11. Stores an additional pickle that contains features-info

See also data-handling diagram here:
https://dagshub.com/Omdena/ThinkOutside/src/master/data/data_pipeline.pptx


### Energy-price data-handling limitations:

With current code implementation, we have the following limitations:

	1. The provided code does NOT contain any flow to automatically fetch the data from any servers / API. The code just assumes data will be available in the raw folder.
	2. For weather data, some extra notebooks are required to be run first to prepare that data before running data_main.py (see linked diagram above for details).