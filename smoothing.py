import pandas as pd
import numpy as np



def func(days,row):
	if (row==(len(days)-1)):
		d=0
	elif (row==0):
		r=days.loc[row,'Nof_days']
		d=np.arange(0,r)
	else:
		I=row-1
		r=days.loc[0:I,'Nof_days'].sum()
		r1=days.loc[row,'Nof_days']
		d=np.arange(r,r1+r)
	return d



def smoothing_func(df):
	df.transaction_date=pd.to_datetime(df.transaction_date)
	df.transaction_date=df.transaction_date.dt.date
	df.sort_values('transaction_date',inplace=True)
	df['Nof_days']=-df.transaction_date.diff(periods=-1).dt.days	
	days=df.groupby('transaction_date')['kWh_sold','Nof_days'].sum()
	days['kWh_per_day']=days.kWh_sold/days.Nof_days
	idx=days.index
	I=len(idx)-1
	days=days.reset_index(drop=True).drop(columns='kWh_sold')
	days['days']=days.apply(lambda rows: func(days,rows.name),axis=1)
	days.days=days.days+1
	days.days[I]=idx[I]
	days=days.explode('days').reset_index(drop=True)
	return days

