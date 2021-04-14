import matplotlib
import numpy as np
matplotlib.use('Agg')
import matplotlib.pyplot as plt 
import seaborn as sns 
import pandas as pd
import sys 
sys.path.append('/home/jtaneja/work/projects/smallcommercial/pckages')
import grph as grh
import pickle
from matplotlib import cbook
#import temp as gr
import yaml

def main(df,parameters_list,installation_date,residential_rows,meta_filepath,drp_col,drp_name,rawORsmoothed):
	#mult_id=pd.read_pickle('/mnt/nfs/work1/jtaneja/jmugyenyi/REG_consumption_data/meter_ids_with_multiple_customers/meter_ids_with_multiple_customers.pck')
	#df.rename(columns={'kWh_sold':'kWhs'},inplace=True)

	consumer_id_col=parameters_list[0]
	consumption_param=parameters_list[1]
	transaction_date_param=parameters_list[2]
	freq=parameters_list[3]
	vending_category_colname=parameters_list[4]

	meta_df=pd.read_pickle(meta_filepath)
	#meta_df=meta_df[~meta_df.meter_serial_number.isin(mult_id)]
	meta_df[installation_date]=pd.to_datetime(meta_df[installation_date])
	if rawORsmoothed=='smoothed':
		df[transaction_date_param]=pd.to_datetime(df[['year','month']].assign(DAY=28))
	else:
		df=df[(df[drp_col].isin(drp_name))]
	
	df=df[df[consumption_param]>=0]
	df[transaction_date_param]=pd.to_datetime(df[transaction_date_param])
	df=pd.merge(df,meta_df, on=[consumer_id_col])
	df=df.explode(vending_category_colname)
	#df=df.explode('district')
	I=df[vending_category_colname].isin(residential_rows)
	df.loc[I,vending_category_colname]='Residential'
	df.loc[~I,vending_category_colname]='Non-Residential'
	days=df[transaction_date_param]-df[installation_date]
	df[freq]=(days/np.timedelta64(1,'M')).round()
	df['year_of_connctn']=df[installation_date].dt.year

	return df


def yr_connction(df,typ,freq,segment,parameters_list,fig_name):
	kWhs=parameters_list[1]
	for name,group in df.groupby(segment):

		group=rmv_duplicates(group)
		Mdn=group.groupby(freq)[kWhs].median().to_frame()
		Mdn['Nof_bills']=group.groupby(freq)[kWhs].count()
		Mdn=Mdn[(Mdn.index>=0)]
		if typ=='median':
			qtl=Mdn.Nof_bills.quantile(0.1)
			Mdn=Mdn[Mdn.Nof_bills>qtl]	
			sns.set(style="whitegrid")
			sns.lineplot(Mdn.index,Mdn[kWhs],label=name)
			plt.legend()
			plt.xlabel('Number of Months after connection')
			plt.ylabel('Median kWh')
		elif typ=='count':
			sns.set(style='whitegrid')
			gr.plt_Nof_bills(Mdn.index,Mdn.Nof_bills,name)
			plt.legend()
		else: print('type of action should be either median or count')
			
	plt.savefig(fig_name);plt.close()


def mdn_consumer(df,month_col,kWhs):	
	mdn_consmer=df.groupby(month_col)[kWhs].median().to_frame()
	mdn_consmer['LowerQuartile']=df.groupby(month_col)[kWhs].quantile(0.25)
	mdn_consmer['UpperQuartile']=df.groupby(month_col)[kWhs].quantile(0.75) 
	mdn_consmer['Nof_bills']=df.groupby(month_col)[kWhs].count()
	mdn_consmer=mdn_consmer[mdn_consmer.index>=0]	
	return mdn_consmer

def rmv_duplicates(df,parameters_list):
	consumer_id=parameters_list[0]
	kWhs=parameters_list[1]
	transaction_date=parameters_list[2]
	month_col=parameters_list[3]
	vending_category_name=parameters_list[4]	
	df_dropedup1=df.drop_duplicates(subset=[consumer_id,kWhs,transaction_date,month_col])
	df_dropedup2=df_dropedup1.groupby([consumer_id,vending_category_name,month_col])[kWhs].sum().reset_index()
	return df_dropedup2
	
def box_plt(df_a,col_grp,figname):
	stats=[]
	for name,group in df_a.groupby(col_grp):
		stats.extend(cbook.boxplot_stats(group['kWhs'],labels=[name]))
	fig, ax = plt.subplots(figsize=(15,10))
	ax.bxp(stats,showmeans=True,showfliers=False)
	ax.set_ylabel("Consumption (kWh)",fontsize=16)
	plt.xticks(rotation=90)
	plt.savefig(figname)
	plt.close()



'This function gets residential and nonresidential transactions'
def get_resnonres(df,residential_name,non_residential_name):
	df_res=df[df.vending_category_name==residential_name]
	df_nonres=df[df.vending_category_name==non_residential_name]
	return df_res,df_nonres


'Function explodes district column getting district for each transaction'
def get_district(df3,df6):
	df3_dsrt=df3.explode('district')
	I1=df3_dsrt.district!='KIGALI'
	df3_dsrt.loc[I1,'district']='OTHERS'
	df6_dsrt=df6.explode('district')	
	I2=df6_dsrt.district!='KIGALI'
	df6_dsrt.loc[I2,'district']='OTHERS'
	return df3_dsrt,df6_dsrt


if __name__=='__main__':
	raw_transactions_path='/mnt/nfs/work1/jtaneja/jmugyenyi/REG_consumption_data/files_from_bob/REG_transaction_data.pck'
	smoothed_path='/mnt/nfs/work1/jtaneja/jmugyenyi/REG_consumption_data/merged_smoothed_monthly_nov092020.pck'	
	meta_filepath=df='/home/jtaneja/work/projects/smallcommercial/REG/REG_metadata.pck'

	with open('config_cols.yaml') as file_discriptor:
	 config_cols=yaml.load(file_discriptor,Loader=yaml.FullLoader)

	consumer_id_col=config_cols['consumer_id_col']
	transaction_date_param = config_cols['transaction_date_param']
	consumption_param = config_cols['consumption_param']
	freq='Nof_Mths'
	vending_category_colname=config_cols['customer_type_colname']
	installation_date=config_cols['installation_date']
	residential_rows=config_cols['residential_rows']
	#non_residential_name=config_cols['customer_type'][vending_category_colname][1]
	rawORsmoothed='raw'


	df_raw=pd.read_pickle(raw_transactions_path)
	df_smoothed=pd.read_pickle(smoothed_path)

	drp_col=list(config_cols['drp_rows'].keys())[0]
	drp_name=config_cols['drp_rows'][drp_col]
	parameters_list=[consumer_id_col,consumption_param,transaction_date_param,freq,vending_category_colname]



	#amnt=pd.read_pickle('REG_transaction_amount_data.pck')
	#amnt.rename(columns={'amount':'kWh_sold'},inplace=True)
	df=main(df_raw,parameters_list,installation_date,residential_rows,meta_filepath,drp_col,drp_name,rawORsmoothed)
	#df_smoothed=main(df_smoothed,freq)
	pickle.dump(df,open('./new_df.pck','wb'))
	df_res,df_nonres=get_resnonres(df,residential_name,non_residential_name)
	

	df=rmv_duplicates(df,parameters_list)
	df_res=rmv_duplicates(df_res,parameters_list)
	df_nonres=rmv_duplicates(df_nonres,parameters_list)

	Mdn_df=mdn_consumer(df,freq,consumption_param) 
	Nonres_Mdn_df=mdn_consumer(df_nonres,freq,consumption_param)
	Res_Mdn_df=mdn_consumer(df_res,freq,consumption_param)	

	#plot residential median and interquatile range
	x=M.index.values[:];y=M.kWhs;y1=M.Q1;y2=M.Q2
	gr.plt_IQR(x,y,y1,y2,'fig5_Residential.png') 	
	
	#plot Non-residential median and intequatile range
	x=M2.index.values[:];y=M2.kWhs;y1=M2.Q1;y2=M2.Q2
	gr.plt_IQR(x,y,y1,y2,'fig5_nonResidential.png')
	
	"Residential year of coonection segments"
	#df3=df_res[(df_res.year_of_connctn>=2012)&(df_res.year_of_connctn<2019)]

	"Non-residential year of coonection segments"
	#df6=df_nonres[(df_nonres.year_of_connctn>=2012)&(df_nonres.year_of_connctn<2019)]
	
	"plots"
	#yr_connction(df3,'median',freq,'year_of_connctn','res_df3.png') 
	
	"get districts for each transaction and plot using yr_connction()"
	#df3_dstr,df6_dsrt=get_district(df3,df6)
	#yr_connction(df3_dsrt[df3_dsrt.district=='KIGALI'],'median',freq,'year_of_connctn','res_df3.png') 

