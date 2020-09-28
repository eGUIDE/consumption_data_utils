import pandas as pd
import numpy as np
import os, pickle
from matplotlib import cbook


def get_data_by_calendar_month(data, transaction_date_param, frequency='by_month'):
    data[transaction_date_param] = pd.to_datetime(data[transaction_date_param])
    data['year'] = data[transaction_date_param].dt.year
    if frequency == 'by_month':
        data[frequency] = data[transaction_date_param].dt.month
    elif frequency == 'by_week':
        data[frequency] = data[transaction_date_param].dt.week
    elif frequency == 'by_day':
        data[frequency] = data[transaction_date_param].dt.day
    else:
        print('Selected {} frequency. Please choose one of the following frequencies:  by_day, by_week, by_month ')
    grouped_data = data.groupby(frequency)
    import ipdb; ipdb.set_trace()
    frequency_summary_for_yr = {} 
    for name, group in  grouped_data:
        token = str(group['year'].iloc[0]) + '-' + str(name) 
        cur_output = cbook.boxplot_stats(group[consumption_param],labels=[token])
        frequency_summary_for_yr[token] = cur_output
    
    return frequency_summary_for_yr

if __name__ == '__main__':
    # meta_datafile = '/media/PantherHDD/nsutezo/data/cons_utils_data/REG_metadata.pck'  # file containing raw data
    transaction_datafile = '/media/PantherHDD/nsutezo/data/cons_utils_data/REG_transaction_data.pck'
    transaction_data = pickle.load(open(transaction_datafile,'rb'))
    # meta_data =  pickle.load(open(meta_datafile,'rb'))

    # drop negative values
    print('There are initially {} entries in the transaction file'.format(transaction_data.shape[0]))
    transaction_data = transaction_data[transaction_data.kWh_sold >=0]
    print('There are {} entries in the transaction file after dropping negative consumptions'.format(transaction_data.shape[0]))


    # remove regulatory & tva 
    transaction_data = transaction_data[~transaction_data.tariff_name.isin(['TVA tax','Regulatory_Fee'])]

    ## residential is :
    ## non-residential is: 

    param = 'transaction_date'
    grouped_data_calendar_date =  get_data_by_calendar_month(transaction_data,param)