import pandas as pd
import numpy as np
from datetime import timedelta
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt 
import matplotlib.dates as mdates


def smoothing_func(df):
	
	if len(df)<1:
		return print('Input dataframe must contain atleast one transaction'),[]
	else:
		df.transaction_date=pd.to_datetime(df.transaction_date)
		df.transaction_date=df.transaction_date.dt.date
		df.sort_values('transaction_date',inplace=True)
		days=df.groupby('transaction_date')['kWh_sold'].sum().reset_index()
	
	if len(days)<=1:
		days['Nof_days']=1
		days.rename(columns={'transaction_date':'days','kWh_sold':'kWh_per_day'},inplace=True)
		earliest_transaction_date =  days.days.iloc[0]
		days.days[0] = 1
	else:
		days['Nof_days']=-days.transaction_date.diff(periods=-1).dt.days	
		medians=days.Nof_days.median()
		days.Nof_days=days.Nof_days.fillna(medians)
		if medians >=10:
			min_=days.Nof_days.min()
			Nof_days=days.Nof_days.clip(min_,medians)
			days['kWh_per_day']=days.kWh_sold/Nof_days
		else:
			days['kWh_per_day']=days.kWh_sold/days.Nof_days
		idx=days.transaction_date
		I=len(idx)-1
		days.drop(columns=['kWh_sold','transaction_date'],inplace=True)
		days['days']=days.Nof_days.apply(lambda rows: np.arange(rows))
		days=days.explode('days').reset_index(drop=True)
		if medians >=10:
			days.loc[days.days>medians,'kWh_per_day']=0
		days.days=days.index.values+1
		earliest_transaction_date = idx.min()
	return days,earliest_transaction_date
