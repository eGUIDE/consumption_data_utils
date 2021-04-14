import pandas as pd
import os,sys,pickle
import numpy as np
import yaml

if __name__ == '__main__':
    
    # Initialize some variables that correspond to data file path, step size, the consumer id column name that we want to use as well as the path where we want to save the batch files.
    step_size = 16000
    
    with open('config_cols.yaml') as file_discriptor:
         config_cols=yaml.load(file_discriptor,Loader=yaml.FullLoader)

    consumer_id_col=config_cols['consumer_id_col']
    consumption_param = config_cols['consumption_param']
    drp_col=list(config_cols['drp_rows'].keys())[0]
    drp_rows=config_cols['drp_rows'][drp_col]
    transaction_datafile_path = config_cols['transaction_datafile_path'] 
    batch_files_path = config_cols['batch_files_path']
    savepath=os.path.join(batch_files_path,'batch_grouped_consumer_ids')
    
    #Load transaction data file from the path entered
    transaction_data = pickle.load(open(transaction_datafile_path,'rb'))
    
    # remove regulatory & tva fees as well as transactions that corespond to negative kWh
    transaction_data = transaction_data[~transaction_data[drp_col].isin(drp_rows)]
    transaction_data = transaction_data[transaction_data[consumption_param]>=0]
    print('There are {} entries in the transaction file after dropping TVA tax, Regulatory_Fee, Rura_fee'.format(transaction_data.shape[0]))
    
    #Save the batches of transactions as pickle files eg transaction_file_batch_1_reg.pck
    if os.path.exists(savepath):
        #shutil.rmtree(savepath)
        pass
    else:
        os.mkdir(savepath)
    unique_consumer_ids = transaction_data[consumer_id_col].unique().tolist()
    length = (len(unique_consumer_ids)//step_size   + 1)* step_size
    for i in np.arange(0,length,step_size):
        start_idx = i
        stop_idx  = start_idx + step_size
        tmp_ids = unique_consumer_ids[start_idx:stop_idx]
        tmp_df =  transaction_data[transaction_data[consumer_id_col].isin(tmp_ids)]
        tmp_df.to_pickle(os.path.join(savepath,'transaction_file_batch_{}_reg.pck'.format(i)))       


















