import pandas as pd
import numpy as np
import os, pickle
from matplotlib import cbook


def get_data_by_calendar_month(data,consumer_id,transaction_date_param,consumption_param,mode = 'all', frequency='by_month', district=None):
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
    grouped_data = data.groupby([frequency,'year'])
    #import ipdb; ipdb.set_trace()
    frequency_summary_for_yr = {} 
    for name, group in  grouped_data:
        agg_output = group.groupby(consumer_id).agg({consumption_param:'sum'})
        #token =  str(name) 
        cur_output = cbook.boxplot_stats(agg_output[consumption_param],labels=[name])
        cur_output[0]['count'] = agg_output.shape[0]
        frequency_summary_for_yr[name] = cur_output[0]        
   
    pickle.dump(frequency_summary_for_yr, open('reg_summary_{}_{}_{}_data.pck'.format(frequency,mode,district),'wb')) 
    

if __name__ == '__main__':
    
    with open('config_cols.yaml') as file_discriptor:
         config_cols=yaml.load(file_discriptor,Loader=yaml.FullLoader) 

    meta_datafile = config_cols['meta_datafile_path']
    #transaction_datafile = '/mnt/nfs/eguide/projects/electricity_prediction/data/REG_data/REG_transaction_data.pck'
    transaction_datafile =config_cols['transaction_datafile_path'] 
    transaction_data = pickle.load(open(transaction_datafile,'rb'))
    meta_data =  pickle.load(open(meta_datafile,'rb'))
    #meta_data = meta_data.explode('District')
    #meta_data['District'] =  meta_data.district.apply(lambda x: x[0].upper())
    # drop negative values
    print('There are initially {} entries in the transaction file'.format(transaction_data.shape[0]))
    #transaction_data = transaction_data[transaction_data.kWh_sold >=0]
    print('There are {} entries in the transaction file after dropping negative consumptions'.format(transaction_data.shape[0]))

    #import ipdb; ipdb.set_trace()
    # remove regulatory & tva 
    drp_rows = config_cols['drp_rows']
    transaction_data = transaction_data[~transaction_data.tariff_name.isin(drp_rows)]
    #print('There are {} entries in the transaction file after dropping TVA tax, Regulatory_Fee, Rura_fee'.format(transaction_data.shape[0]))

    #merged_data = pd.merge(transaction_data,meta_data, on=['meter_serial_number','consumer_id'])
    #del transaction_data
    #del meta_data
    #merged_data = merged_data.explode('vending_category_name')
    #merged_data = merged_data[~merged_data.vending_category_name.isin(['10. Residential','2. T1 Tx FC1 AR STS'])]
    #print('There are {} entries in the transaction file with residential consumption'.format(merged_data.shape[0]))
    #pickle.dump(transaction_data, open('data/actual_consumption_data_reg.pck','wb'))
    ## residential is :
    ## non-residential is: 
    #del transaction_data
    #del meta_data
    consumer_id_col=config_cols['consumer_id_col']
    transaction_date_param = config_cols['transaction_date_param']
    consumption_param = config_cols['consumption_param']
    #consumption_param = 'kWh_sold'
    frequency = 'by_month'
    mode = 'fixed_amounts'
    district = 'all'
    
    #for district in meta_data.District.unique():
    #    cur_meta =  meta_data[meta_data.District == district][['meter_serial_number','consumer_id','District']]
    #    merged_data = pd.merge(transaction_data,cur_meta, on=['meter_serial_number','consumer_id'])
    #    print('For district {}, there are {} transactions'.format(district,merged_data.shape[0]))
    #    get_data_by_calendar_month(merged_data,transaction_param,consumption_param,mode,frequency,district)
    get_data_by_calendar_month(transaction_data,consumer_id_col,transaction_date_param,consumption_param,mode,frequency,district)
