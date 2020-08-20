import pandas as pd
import os,pickle,shutil
from tqdm import tqdm
import numpy as np
import multiprocessing
import matplotlib.pyplot as plt
from functools import partial
from matplotlib import cbook
import datetime

def loggers(log_filename,data, params=None):
    try:
        f = open(log_filename)
        data.to_csv(log_filename, mode='a', header=False)
        #with open(log_filename, 'a') as src:
        #    src.writelines(data)
            #for k in range(len(data)-1):
            #    src.write(str(data[k]) + ",")
            #src.write(str(data[-1]) +"\n")
    except Exception as err:
        print('{} File did not exist but has been created and the data written to it'.format(log_filename))
        data.to_csv(log_filename)
        #with open(log_filename, 'w') as src:
            #print(params)
         #   for k in range(len(params)-1):
         #       src.write(str(params[k]) + ",")
         #   src.write(str(params[-1])+"\n")
         #   src.writelines(data)
            #src.write(str(params) + "\n")
            #for k in range(len(data)-1):
            #    src.write(str(data[k]) + ",")
            #src.write(str(data[-1]) +"\n")


def get_data_size(datafile,chunksize=1e6,buffersize=0):
    '''
    This file computes the total number of entries in the data
    datafile:  electricity data transaction file
    chunksize: how much of the data to read at each point, Default is 1e-6
    buffersize: already read entries
    '''
    df =  pd.read_csv(datafile, nrows=chunksize,skiprows = buffersize)
    buffersize =  int(np.floor(os.path.getsize(datafile) / df.memory_usage(index=True).sum()))
    print('Dataset has would sliced into {} chunks and parallel proccessed '.format(buffersize))
    return buffersize, df.columns

def read_file_write_to_chunks(tmp_output_path,datafile,transaction_date_param,params,columns,chunksize,chunk_idx):
    print('Writing to csv for chunk idx {}'.format(chunk_idx))
    try:
        chunk = pd.read_csv(datafile, skiprows=int(chunksize*chunk_idx),nrows=chunksize)
        chunk.columns = columns
        chunk[transaction_date_param] =  pd.to_datetime(chunk[transaction_date_param])
        chunk['year'] = chunk[transaction_date_param].dt.year
        grouped_by_year = chunk.groupby('year')
        for name, group in grouped_by_year:
            path_token = os.path.join(tmp_output_path,'transactions_for_chunkidx_{}_year_{}_.csv'.format(chunk_idx,name))
        
            group[params].to_csv(path_token, header=False)
            
    except Exception as err:
        print('Error for chunk {}: '.format(chunk_idx),err)

def merge_chunk_data_by_year_chunks(tmp_output_path,output_data_dir_by_year,params,year):
    relevant_files =  os.listdir(tmp_output_path)
    summary_df = pd.DataFrame(columns=params)
    summary_df.to_csv(os.path.join(output_data_dir_by_year ,'transactions_for_year_{}_.csv'.format(year))) 
    for curfile in relevant_files:
        if str(year) in curfile:
            cur_df = pd.read_csv(os.path.join(tmp_output_path,curfile), sep=",")
            cur_df[params].to_csv(os.path.join(output_data_dir_by_year,'transactions_for_year_{}_.csv'.format(year)),mode='a',header=False)

def load_data(output_data_dir_by_year,datafile,params,transaction_date_param,workers=1,chunksize=1000000):
    '''
    This function loads data in chunks, writes outputs to text file by year
    output_data_dir_by_year : path to write out data by year
    datafilename: electricity transaction file
    params: the parameters to write out for each year
    chunksize: file size to load (adjust accordingt to system)
    '''
    print('Starting the process of separating transactions by year')
    tmp_folder = 'chunking_tmp_folder'

    num_file_entries, column_names = get_data_size(datafile,chunksize)
    num_file_read_idxs = [k for k in range(num_file_entries)]
    
    if os.path.exists(tmp_folder):
        shutil.rmtree(tmp_folder)
    os.mkdir(tmp_folder)

    
    p = multiprocessing.Pool(processes=workers)
    read_file_write_to_chunks_func = partial(read_file_write_to_chunks,tmp_folder,datafile,transaction_date_param,params,column_names,chunksize)
    p.map(read_file_write_to_chunks_func,num_file_read_idxs)
    p.close()
    p.join()
    
   # aggregate all txt files by year and delete tmp_dir
    chunks_by_year = os.listdir(tmp_folder)
    years =  [int(k.split('_')[-2]) for k in chunks_by_year]
    years =  np.unique(years)
    if workers > len(years):
        workers =len(years)

    p = multiprocessing.Pool(processes=workers)
    merge_chunk_data_by_year_chunks_func = partial(merge_chunk_data_by_year_chunks,tmp_folder,output_data_dir_by_year,params)
    p.map(merge_chunk_data_by_year_chunks_func,years)
    p.close()
    p.join()

    #delete tmp directory
    shutil.rmtree(tmp_folder)

    print('Done with separating transactions by year')

        
def get_aggregation(frequency,transaction_date_param,consumption_param, summary_folder,curfile):
    '''
    '''
    data = pd.read_csv(curfile, sep=",")
    year = curfile.split('_')[-2]
    data[transaction_date_param] = pd.to_datetime(data[transaction_date_param])
    if frequency == 'by_month':
        data[frequency] = data[transaction_date_param].dt.month
    elif frequency == 'by_week':
        data[frequency] = data[transaction_date_param].dt.week
    elif frequency == 'by_day':
        data[frequency] = data[transaction_date_param].dt.day
    else:
        print('Selected {} frequency. Please choose one of the following frequencies:  by_day, by_week, by_month ')
    grouped_data = data.groupby(frequency)

    frequency_summary_for_yr = {} 
    for name, group in  grouped_data:
        token = str(year) + '-' + str(name) 
        cur_output = cbook.boxplot_stats(group[consumption_param],labels=[token])
        frequency_summary_for_yr[token] = cur_output

    savepath = os.path.join(summary_folder, 'transactions_summary_frequency_{}_year_{}_.pck'.format(frequency,curfile.split('_')[-2]))
    pickle.dump(frequency_summary_for_yr,open(savepath,'wb'))
    print('Summarized data at {} frequency for file : {}'.format(frequency,curfile))
    print('{} entries are present in the file'.format(len(list(frequency_summary_for_yr))))

def aggregate_data_by_frequency(output_data_dir_by_year,aggregation_summary_folder,transaction_date_param,consumption_param , frequency='by_month',workers=1):
     ''' 
     This file aggregates each year file by the desired frequency
     frequency: aggregation frequency e.g. by_month, by_week. Default is by_month
     transaction_date_param: the parameter that corresponds to transaction dates
     consumption_param: parameter that corresponds to electricity consumption
     aggregation_summary_folder: the directory for the aggregated data by frequency
     output_data_dir_by_year: the directory that holds transaction data by year
     '''
     files_by_year = os.listdir(output_data_dir_by_year)
     files_by_year = [os.path.join(output_data_dir_by_year,k) for k in files_by_year]
     #for k in files_by_year:
     #    print(k)
     #    get_aggregation(frequency, transaction_date_param,consumption_param,aggregation_summary_folder,k)
     p = multiprocessing.Pool(processes=workers)
     tmp_func = partial(get_aggregation,frequency, transaction_date_param,consumption_param,aggregation_summary_folder)
     p.map(tmp_func,files_by_year)
     p.close()
     p.join()
    

def plot_temporal_by_frequency(agg_summarize_pathname_by_year_and_frequency, figname = 'date_timeseries.png'):
    all_files = sorted(os.listdir(agg_summarize_pathname_by_year_and_frequency))
    all_files = [os.path.join(agg_summarize_pathname_by_year_and_frequency,k) for k in all_files]
    ordered_data = {}
    for curfile in all_files:
        data = pickle.load(open(curfile,'rb'))
        for k , v in data.items():
            ordered_data[k]=v
    sorted_keys_raw = sorted(list(ordered_data.keys()))
    sorted_dates = [pd.to_datetime(k) for k in sorted_keys_raw]
    sorted_dates_idx = np.argsort(sorted_dates)
    sorted_keys = [sorted_keys_raw[k] for k in sorted_dates_idx]
    sorted_values = []
    for cur_key in sorted_keys:
        sorted_values.append(ordered_data[cur_key][0])
    fig, ax = plt.subplots(figsize=(15,15));
    ax.bxp(sorted_values, showfliers=False);
    ax.set_ylabel("Consumption (kWh)",fontsize=16);
    ax.set_xticklabels(sorted_keys,rotation=90);
    plt.savefig(figname)
    plt.show()


if __name__ == '__main__':
    raw_data = '../cons_utils_data/reg_data_combined.csv'  # file containing raw data
# df = pd.read_csv(raw_data)
    data_dir_transactions_by_year = '../cons_utils_data/transactions_by_year'
    parameters = ['consumer_id','transaction_date','kWh_sold'] # parameters to store when separating data by year
    transaction_date_parameter = 'transaction_date'
    consumption_parameter = 'kWh_sold'
    frequency = 'by_month' # frequency of aggregation
    aggregation_transactions_by_yr_and_frequency = '../cons_utils_data/transactions_by_year_and_freq'
    workers = 32
    print('Start time is : ', datetime.datetime.now())
    load_data(data_dir_transactions_by_year,raw_data,parameters, transaction_date_parameter,workers)
    print('End time is : ', datetime.datetime.now())
