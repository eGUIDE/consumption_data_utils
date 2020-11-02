import pandas as pd
import numpy as np
from datetime import timedelta
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt 
import matplotlib.dates as mdates

def func(days,row):
	#if (row==(len(days)-1)):
	#	d=0
	if (row==0):
		r=days.loc[row,'Nof_days']
		d=np.arange(0,r)
	else:
		I=row-1
		r=days.loc[0:I,'Nof_days'].sum()
		r1=days.loc[row,'Nof_days']
		d=np.arange(r,r1+r)
	return d



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
		
		#days['days']=days.apply(lambda rows: func(days,rows.name),axis=1)
		#days.days=days.days+1
		days=days.explode('days').reset_index(drop=True)
		if medians >=10:
			days.loc[days.days>medians,'kWh_per_day']=0
	days.days=days.index.values+1
	return days,idx[0]

