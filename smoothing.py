import pandas as pd
import numpy as np
from datetime import timedelta
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt 
import matplotlib.dates as mdates
import seaborn as sns 
import sys 
sys.path.append('/home/jtaneja/work/projects/smallcommercial/pckages')
import grph as grh

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



def days_means_medians(S):
	m=S.sort_values().diff()
	if len(m)>1:
		m=m.dt.days
	else:
		m=0
	means=np.nanmean(m)
	medians=np.nanmedian(m)
	return (means,medians)


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
		
		#days['days']=days.apply(lambda rows: func(days,rows.name),axis=1)
		#days.days=days.days+1
		days=days.explode('days').reset_index(drop=True)
		if medians >=10:
			days.loc[days.days>medians,'kWh_per_day']=0
		days.days=days.index.values+1
		earliest_transaction_date = idx.min()
	return days,earliest_transaction_date


DF=pd.read_pickle('/home/jtaneja/work/projects/smallcommercial/REG/REG_transaction_data.pck')
DF=DF[DF.kWh_sold>=0]
drp_name=['TVA tax','Regulatory_Fee','Rura_fee']
DF=DF[~(DF.tariff_name.isin(drp_name))]
DF.transaction_date=pd.to_datetime(DF.transaction_date)
DF.transaction_date=DF.transaction_date.dt.date
DF=DF.groupby(['consumer_id','meter_serial_number','transaction_date']).kWh_sold.sum().reset_index()
a=DF.groupby(['consumer_id','meter_serial_number']).kWh_sold.count()
df1=DF[DF.consumer_id=='20121101 104121.689 0002032 0025']
df2=DF[DF.consumer_id=='57114']
q=smoothing_func(df1)
n=len(q)-1
r2=q.days[n] 
r1=r2-timedelta(n)
q.days=pd.date_range(start=r1,end=r2,freq='D')


def plt_smooth(df1,q):
	sns.set(style='whitegrid')
	fig,ax=plt.subplots(figsize=(20,10))
	ax.scatter(df1.transaction_date,df1.kWh_sold,label='original')
	ax.scatter(q.months,q.kWh_per_day,label='smoothed_monthly_aggregate',color='r')
	plt.legend()
	plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
	plt.gcf().autofmt_xdate()
	plt.ylabel('kWh used')
	plt.title('Comparing original prepaid kWh consumption with daily smoothed-out estimates')
	plt.savefig('fig1.png')
	plt.close()
