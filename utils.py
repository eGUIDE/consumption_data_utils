import pandas as pd
import os,pickle
from tqdm import tqdm
import numpy as np
import multiprocessing
import matplotlib.pyplot as plt
from functools import partial
from matplotlib import cbook

def loggers(log_filename,data, params=None):
    try:
        f = open(log_filename)
        with open(log_filename, 'a') as src:
            for k in range(len(data)-1):
                src.write(str(data[k]) + ",")
            src.write(str(data[-1]) +"\n")
    except Exception as err:
        print('{} File did not exist but has been created and the data written to it'.format(log_filename))
        with open(log_filename, 'w') as src:
            #print(params)
            for k in range(len(params)-1):
                src.write(str(params[k]) + ",")
            src.write(str(params[-1])+"\n")
            #src.write(str(params) + "\n")
            for k in range(len(data)-1):
                src.write(str(data[k]) + ",")
            src.write(str(data[-1]) +"\n")

def load_data(output_data_dir_by_year,datafile,params,transaction_date_param,chunksize=1000000):
    '''
    This function loads data in chunks, writes outputs to text file by year
    output_data_dir_by_year : path to write out data by year
    datafilename: electricity transaction file
    params: the parameters to write out for each year
    chunksize: file size to load (adjust accordingt to system)
    '''
    print('Starting the process of separating transactions by year')
    for chunk in tqdm(pd.read_csv(datafile, chunksize=chunksize)):
        chunk[transaction_date_param] =  pd.to_datetime(chunk[transaction_date_param])
        for idx , row in chunk.iterrows():
            loggers(os.path.join(output_data_dir_by_year,'transactions_for_year_{}_.txt'.format(row[transaction_date_param].year)),
                 row[params].tolist(),params)
    print('Done with separating transactions by year')
        
def get_aggregation(frequency,transaction_date_param,consumption_param, summary_folder,curfile):
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
