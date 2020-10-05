import pandas as pd
import numpy as np



def func(days,row):
	if (row==0):
		d=0
	elif (row==1):
		r=days.loc[row,'transaction_date']
		d=np.arange(1,r)
	else:
		I=row-1
		r=days.loc[1:I,'transaction_date'].sum()
		r1=days.loc[row,'transaction_date']
		d=np.arange(r,r1+r)
	return d



def smoothing_func(df):
	df.transaction_date=pd.to_datetime(df.transaction_date)
	df.sort_values('transaction_date',inplace=True)
	days=df.transaction_date.diff().dt.days.to_frame().replace(0,1)
	days['kWh_sold']=df.kWh_sold/days.transaction_date
	days=days.reset_index().drop(columns='index')
	days['days']=days.apply(lambda rows: func(days,rows.name),axis=1)
	return days
