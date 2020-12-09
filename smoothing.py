import pandas as pd
import numpy as np
from datetime import timedelta
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt 
import matplotlib.dates as mdates




def smoothing_func(df,frq):
	
	if len(df)<1:
		print('Input dataframe must contain atleast one transaction')
		return None, None

	#return print('Input dataframe must contain atleast one transaction'),[]
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
		days_daily = days
	else:
		days['Nof_days']=-days.transaction_date.diff(periods=-1).dt.days	
		medians=days.Nof_days.median()
		days.Nof_days=days.Nof_days.fillna(medians)
		if (medians >=frq)&(medians<100):
			min_=days.Nof_days.min()
			Nof_days=days.Nof_days.clip(min_,medians)
			days['kWh_per_day']=days.kWh_sold/Nof_days
			earliest_transaction_date = days.transaction_date.min()
			days['days']=days.Nof_days.apply(lambda rows: np.arange(rows))
			days_daily=days.explode('days').reset_index(drop=True)
			days_daily=days_daily[days_daily.days<=medians]
			days_daily.days=days_daily.index.values+1
		elif medians>=100:
			min_=days.Nof_days.min()
			Nof_days=days.Nof_days.clip(min_,100)
			days['kWh_per_day']=days.kWh_sold/Nof_days
			earliest_transaction_date = days.transaction_date.min()
			days['days']=days.Nof_days.apply(lambda rows: np.arange(rows))
			days_daily=days.explode('days').reset_index(drop=True)
			days_daily=days_daily[days_daily.days<=100]
			days_daily.days=days_daily.index.values+1
		else:
			normlzd=days.kWh_sold/days.Nof_days
			qtl=normlzd.quantile(0.1)
			I=days[normlzd<qtl].index
			Nof_days=days.Nof_days.copy()
			Nof_days[I]=np.ceil(days.kWh_sold[I]/qtl)
			days['normlzd']=Nof_days
			max_=normlzd.max()
			days['kWh_per_day']=normlzd.clip(qtl,max_)
			earliest_transaction_date = days.transaction_date.min()
			days['days']=days.Nof_days.apply(lambda rows: np.arange(rows))
			days_daily=days.explode('days').reset_index(drop=True)
			days_daily=days_daily[days_daily.normlzd>days_daily.days]
			days_daily.days=days_daily.index.values+1
			days_daily.drop(columns='normlzd',inplace=True)
		days_daily.drop(columns=['kWh_sold','transaction_date'],inplace=True)
	return days_daily,earliest_transaction_date





#def smoothing_func(df,frq):
	
#	if len(df)<1:
#		print('Input dataframe must contain atleast one transaction')
#		return None, None
#	else:
#		df.transaction_date=pd.to_datetime(df.transaction_date)
#		df.transaction_date=df.transaction_date.dt.date
#		df.sort_values('transaction_date',inplace=True)
#		days=df.groupby('transaction_date')['kWh_sold'].sum().reset_index()
	
#	if len(days)<=1:
#		days['Nof_days']=1
#		days.rename(columns={'transaction_date':'days','kWh_sold':'kWh_per_day'},inplace=True)
#		earliest_transaction_date =  days.days.iloc[0]
#		days.days[0] = 1
#		days_daily = days
#	else:
#		days['Nof_days']=-days.transaction_date.diff(periods=-1).dt.days	
#		medians=days.Nof_days.median()
#		days.Nof_days=days.Nof_days.fillna(medians)
#		if medians >=frq:
#			min_=days.Nof_days.min()
#			Nof_days=days.Nof_days.clip(min_,medians)
#			days['kWh_per_day']=days.kWh_sold/Nof_days
#		else:
#			normlzd=days.kWh_sold/days.Nof_days
#			qtl=normlzd.quantile(0.1)
#			I=days[normlzd<qtl].index
#			Nof_days=days.Nof_days.copy()
#			Nof_days[I]=np.ceil(days.kWh_sold[I]/qtl)
#			days['normlzd']=Nof_days
#			max_=normlzd.max()
#			days['kWh_per_day']=normlzd.clip(qtl,max_)
#		earliest_transaction_date = days.transaction_date.min()
#		days['days']=days.Nof_days.apply(lambda rows: np.arange(rows))
#		days_daily=days.explode('days').reset_index(drop=True)
#		if medians >=frq:
#			days_daily=days_daily[days_daily.days<=medians]
#			days_daily.days=days_daily.index.values+1
#		else:
#			days_daily=days_daily[days_daily.normlzd>days_daily.days]
#			days_daily.days=days_daily.index.values+1
#			days_daily.drop(columns='normlzd',inplace=True)
#		days_daily.drop(columns=['kWh_sold','transaction_date'],inplace=True)
#	return days_daily,earliest_transaction_date





#def func(days,row):
#	if (row==0):
#		r=days.loc[row,'Nof_days']
#		d=np.arange(0,r)
#	else:
#		I=row-1
#		r=days.loc[0:I,'Nof_days'].sum()
#		r1=days.loc[row,'Nof_days']
#		d=np.arange(r,r1+r)
#	return d




#def smoothing_func(df):
#	df.transaction_date=pd.to_datetime(df.transaction_date)
#	df.transaction_date=df.transaction_date.dt.date
#	df.sort_values('transaction_date',inplace=True)
#	days=df.groupby('transaction_date')['kWh_sold'].sum().reset_index()
	
#	if len(days)<=1:
#		days['Nof_days'] = 1
#		days.rename(columns={'transaction_date':'days','kWh_sold':'kWh_per_day'},inplace=True)
#		earliest_transaction_date =  days.days.iloc[0]
#		days.days[0] = 1

#	else:
#		days['Nof_days']=-days.transaction_date.diff(periods=-1).dt.days	
#		medians=days.Nof_days.median()
#		days.Nof_days=days.Nof_days.fillna(medians)
#		if medians >=10:
#			min_=days.Nof_days.min()
#			Nof_days=days.Nof_days.clip(min_,medians)
#			days['kWh_per_day']=days.kWh_sold/Nof_days
#		else:
#			days['kWh_per_day']=days.kWh_sold/days.Nof_days
#		idx=days.transaction_date
#		I=len(idx)-1
#		days.drop(columns=['kWh_sold','transaction_date'],inplace=True)
#		days['days']=days.apply(lambda rows: func(days,rows.name),axis=1)
#		days.days=days.days+1
#		days=days.explode('days').reset_index(drop=True)
#		earliest_transaction_date = idx.min()
#	return days, earliest_transaction_date

