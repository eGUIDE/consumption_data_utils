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



def days_means_medians(S):
	m=S.sort_values().diff().dt.days
	means=m.mean()
	medians=m.median()
	return (means,medians)


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
	days=days.reset_index(drop=True)
	return days


DF=pd.read_pickle('/home/jtaneja/work/projects/smallcommercial/REG/REG_transaction_data.pck')
DF=DF[DF.kWh_sold>=0]
drp_name=['TVA tax','Regulatory_Fee','Rura_fee']
DF=DF[~(DF.tariff_name.isin(drp_name))]
DF.transaction_date=pd.to_datetime(DF.transaction_date)
DF.transaction_date=DF.transaction_date.dt.date
a=DF.groupby(['consumer_id','meter_serial_number']).kWh_sold.count()
df1=DF[DF.consumer_id=='20121101 104121.689 0002032 0025']
df2=DF[DF.consumer_id=='57114']
