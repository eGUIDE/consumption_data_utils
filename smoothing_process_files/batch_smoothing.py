import pandas as pd
import numpy as np
from datetime import timedelta
import smoothing as sm
import os,pickle,sys,shutil
import yaml

def get_monthly_profile(daily_profile,consumer_id,transaction_start_date):
    daily_profile['dates'] = daily_profile['days'].apply(lambda x: transaction_start_date + pd.DateOffset(days=x))
    daily_profile['year'] = daily_profile.dates.dt.year
    daily_profile['month'] =  daily_profile.dates.dt.month
    grouped_data = daily_profile.groupby(['month','year']).agg('sum').reset_index()
    grouped_data['consumer_id'] = consumer_id
    return grouped_data['consumer_id'].tolist(),grouped_data['month'].tolist(),grouped_data['year'].tolist(),grouped_data['per_day'].tolist()
    


if __name__ == '__main__':

    filename = sys.argv[1]
    with open('config_cols.yaml') as file_discriptor:
         config_cols=yaml.load(file_discriptor,Loader=yaml.FullLoader)

    consumer_id_col=config_cols['consumer_id_col']
    transaction_date_param = config_cols['transaction_date_param']
    consumption_param = config_cols['consumption_param']
    freq_threshold = config_cols['freq_threshold']
    
    mypath = os.path.join(config_cols['batch_files_path'],'batch_grouped_consumer_ids')
    df = pd.read_pickle(os.path.join(mypath,filename))
    unique_consumers = df[consumer_id_col].unique().tolist()
    savepath = os.path.join(config_cols['batch_files_path'],'smoothed_monthly_data')


    consumer_ids = []
    months = []
    years = []
    kWhs  = []
    #days = []
    print('Starting file:',filename)
    for idx, item in enumerate(unique_consumers):
        tmp_df = df[df[consumer_id_col] == item]
        daily_profile, first_transaction_date = sm.smoothing_func(tmp_df,[transaction_date_param,consumption_param],freq_threshold) 
        consumer_id, month,year,consumption = get_monthly_profile(daily_profile,item,first_transaction_date)
        consumer_ids.append(consumer_id)
        months.append(month)
        years.append(year)
        kWhs.append(consumption)
        #days.append(day)

    consumer_ids = [item for sublist in consumer_ids for item in sublist]
    months = [item for sublist in months for item in sublist]
    years = [item for sublist in years for item in sublist]
    kWhs = [item for sublist in kWhs for item in sublist]
    #days = [item for sublist in days for item in sublist]


    monthly_profiles = pd.DataFrame(columns=[consumer_id_col,'month','year','kWhs'])
    monthly_profiles[consumer_id_col] = consumer_ids
    monthly_profiles['month'] = months
    monthly_profiles['year'] = years
    monthly_profiles['kWhs'] = kWhs
    #monthly_profiles['days'] =  days

    if os.path.exists(savepath):
       # shutil.rmtree(savepath)
      	pass
    else:
	
         os.mkdir(savepath)
    
    pickle.dump(monthly_profiles,open(os.path.join(savepath,filename),'wb'))
   # monthly_profiles.to_pickle(os.path.join(savepath,filename))
    print('Done with all items in file')
