import pandas as pd
import numpy as np
from datetime import timedelta
import smoothing as sm
import os, sys


def get_monthly_profile(daily_profile,consumer_id,transaction_start_date):
    daily_profile['dates'] = daily_profile['days'].apply(lambda x: transaction_start_date + pd.DateOffset(days=x))
    daily_profile['year'] = daily_profile.dates.dt.year
    daily_profile['month'] =  daily_profile.dates.dt.month
    grouped_data = daily_profile.groupby(['month','year']).agg('sum').reset_index()
    grouped_data['consumer_id'] = consumer_id
    return grouped_data['consumer_id'].tolist(),grouped_data['month'].tolist(),grouped_data['year'].tolist(),grouped_data['kWh_per_day'].tolist()
    


if __name__ == '__main__':
    filename = sys.argv[1]
    mypath = '/mnt/nfs/eguide/projects/electricity_prediction/data/REG_data/smoothing_process/batch_grouped_consumer_ids'
    df = pd.read_pickle(os.path.join(mypath,filename))
    unique_consumers = df.consumer_id.unique().tolist()
    savepath = '/mnt/nfs/eguide/projects/electricity_prediction/data/REG_data/smoothing_process/smoothed_monthly_data'
    consumer_ids = []
    months = []
    years = []
    kWhs  = []
    threshold = 3
    #days = []
    print('Starting file:',filename)
    for idx, item in enumerate(unique_consumers):
        tmp_df = df[df.consumer_id == item]
        daily_profile, first_transaction_date = sm.smoothing_func(tmp_df,threshold) 
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


    monthly_profiles = pd.DataFrame(columns=['consumer_id','month','year','kWhs'])
    monthly_profiles['consumer_id'] = consumer_ids
    monthly_profiles['month'] = months
    monthly_profiles['year'] = years
    monthly_profiles['kWhs'] = kWhs
    #monthly_profiles['days'] =  days

    monthly_profiles.to_pickle(os.path.join(savepath,filename))
    print('Done with all items in file')
