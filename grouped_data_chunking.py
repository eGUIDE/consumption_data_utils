import pandas as pd
import os,sys,pickle
import numpy as np

if __name__ == '__main__':
    # get customers in this block based on id
    step_size = 16000
    transaction_datafile = '/mnt/nfs/eguide/projects/electricity_prediction/data/REG_data/REG_transaction_data.pck'
    transaction_data = pickle.load(open(transaction_datafile,'rb'))
    # remove regulatory & tva 
    transaction_data = transaction_data[~transaction_data.tariff_name.isin(['TVA tax','Regulatory_Fee','Rura_fee'])]
    transaction_data = transaction_data[transaction_data.kWh_sold>=0]
    print('There are {} entries in the transaction file after dropping TVA tax, Regulatory_Fee, Rura_fee'.format(transaction_data.shape[0]))

    unique_consumer_ids = transaction_data.consumer_id.unique().tolist()
    length = (len(unique_consumer_ids)//step_size   + 1)* step_size
    for i in np.arange(0,length,step_size):
        start_idx = i
        stop_idx  = start_idx + step_size
        tmp_ids = unique_consumer_ids[start_idx:stop_idx]
        tmp_df =  transaction_data[transaction_data.consumer_id.isin(tmp_ids)]
        tmp_df.to_pickle(os.path.join('/mnt/nfs/eguide/projects/electricity_prediction/data/REG_data/smoothing_process/batch_grouped_consumer_ids','transaction_file_batch_{}_reg.pck'.format(i)))       


















