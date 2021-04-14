import pandas as pd
import os,pickle,shutil
import numpy as np
from functools import partial
from multiprocessing import Pool
import time



#The function saves pop as a pickle named ppk in present directory
def save_pckle(pop,ppk):
        file=open(ppk,'wb')
        pickle.dump(pop,file,protocol=4)
        file.close()


#The function reads all the pickle files in tmp_dir and concatnates them into a single pickle file called output_file
def read_pickle(tmp_dir):
	Data=pd.DataFrame()
	path=tmp_dir
	for root,dirs,files in os.walk(path):
    		for x in files:
        		D=pd.read_pickle(os.path.join(root,x))
        		#Data=Data.combine_first(D)
       			Data=pd.concat([Data,D],ignore_index=True)
    
	return Data



#This manipulates the different columns for the meta file
def meta_cols(df,date_threshold):
	#meta_obj=df.groupby(['consumer_id','meter_serial_number'])
	meta_obj=df.groupby(['consumer_id','meter_serial_number'])
	meta_df=pd.DataFrame(data={'vending_category_name':meta_obj.vending_category_name.unique()})
	meta_df['district_id']=meta_obj.district_id.unique()
	meta_df['district']=meta_obj.district.unique()
	installation_dates=meta_obj.installation_date.min()
	min_transaction_dates=meta_obj.transaction_date.min()
	meta_df['installation_date']=pd.concat([min_transaction_dates,installation_dates],axis=1).min(axis=1)	
	matched=loc_match(min_transaction_dates,date_threshold)
	matched.set_index(['consumer_id','meter_serial_number'],inplace=True)
	meta_df.loc[matched.index,'Matched_2_location']='YES'
	meta_df.loc[~(meta_df.Matched_2_location=='YES'),'Matched_2_location']='NO'
	meta_df.reset_index(inplace=True)
	return meta_df


#This function finds those customers with unique meter numbers and as such have locations matched in the locations database as of the date_threshold set
def loc_match(min_transaction_dates,date_threshold):
	transaction_dates=min_transaction_dates.reset_index()
	obj=transaction_dates.groupby('meter_serial_number')
	matched=obj.apply(lambda x:x.loc[np.abs(pd.to_datetime(x.transaction_date)-date_threshold).idxmin(),:])
	matched.reset_index(drop=True,inplace=True)
	return matched

#This function writes the chunks for the meta data file
def func_meta(file_path,chunksize,cols,cols2,ids):
	try:
		if ids==0:
			df=pd.read_csv(file_path,nrows=chunksize)
		else:
			df=pd.read_csv(file_path,nrows=chunksize,skiprows=int(chunksize*ids))
			df.columns=cols
		df.consumer_id=df.consumer_id.astype(str).str.strip()		
		df.meter_serial_number=df.meter_serial_number.astype(str)
		#meta_df=meta_cols(df)
		meta_df=df[cols2]
		save_pckle(meta_df,'tmp_meta_dir/meta_chunk_%s.pck'%str(ids))		
	except Exception as err:
		print(err)

#This function writes the chunks for the transactions data file
def func(file_path,chunksize,cols,selctn_cols,tmp_folder,ids):

	try:
		if ids==0:
			df=pd.read_csv(file_path,nrows=chunksize)
		else:
			df=pd.read_csv(file_path,nrows=chunksize,skiprows=int(chunksize*ids))
			df.columns=cols
		df.consumer_id=df.consumer_id.astype(str).str.strip()
		df.meter_serial_number=df.meter_serial_number.astype(str)
		df2=df[selctn_cols]
		save_pckle(df2,tmp_folder+'/chunk_%s.pck'%str(ids))		
	except Exception as err:
		print(err)
		

#The function loads transaction data using multiprocess
def load_transaction_tmps(file_path,chunksize):

	tmp_folder='tmp_dir'
	df=pd.read_csv(file_path,nrows=chunksize)
	cols=df.columns
	selctn_cols=['consumer_id','meter_serial_number','transaction_date','tariff_name','kWh_sold','vending_category_name']
	nof_chunks=int(os.path.getsize(file_path)/(df.memory_usage(index=True).sum()))
	chunk_ids=list(range(nof_chunks))
	
	if os.path.exists(tmp_folder):
		shutil.rmtree(tmp_folder)
	os.mkdir(tmp_folder)

	partial_func=partial(func,file_path,chunksize,cols,selctn_cols,tmp_folder)
	
	process=Pool()
	process.map(partial_func,chunk_ids)
	process.close()
	process.join()

	categoricals=['consumer_id', 'meter_serial_number','tariff_name','vending_category_name']
	load_transaction_df(tmp_folder,categoricals,'REG_transactions.pck')
#	shutil.rmtree(tmp_folder)


def load_amounts_tmps(file_path,chunksize):

	tmp_folder='tmp_amounts_dir'
	df=pd.read_csv(file_path,nrows=chunksize)
	cols=df.columns
	selctn_cols=['consumer_id','meter_serial_number','transaction_date','tariff_name','block_amount','kWh_sold','amount','vending_category_name']
	nof_chunks=int(os.path.getsize(file_path)/(df.memory_usage(index=True).sum()))
	chunk_ids=list(range(nof_chunks))
	
	if os.path.exists(tmp_folder):
		shutil.rmtree(tmp_folder)
	os.mkdir(tmp_folder)

	partial_func=partial(func,file_path,chunksize,cols,selctn_cols,tmp_folder)
	
	process=Pool()
	process.map(partial_func,chunk_ids)
	process.close()
	process.join()

	categoricals=['consumer_id', 'meter_serial_number','tariff_name','vending_category_name']
	load_transaction_df(tmp_folder,categoricals,'REG_transaction_amount_new_data.pck')
#	shutil.rmtree(tmp_folder)


def load_transaction_df(tmp_folder,categoricals,saved_path):
	trans_df=read_pickle(tmp_folder)
	I=trans_df.vending_category_name.isin(['10. Residential','2. T1 Tx FC1 AR STS'])
	trans_df.loc[I,'vending_category_name']='Residential'
	trans_df.loc[~I,'vending_category_name']='Non-Residential'
	trans_df[categoricals]=trans_df[categoricals].astype('category')
	save_pckle(trans_df,saved_path)
	



#The function loads meta data file using multiprocess
def load_meta_tmps(file_path,chunksize):
	tmp_folder='tmp_meta_dir'
	df=pd.read_csv(file_path,nrows=chunksize)
	cols=df.columns
	cols2=['consumer_id','meter_serial_number','transaction_date','installation_date','vending_category_name','district_id','district']
	nof_chunks=int(os.path.getsize(file_path)/(df.memory_usage(index=True).sum()))
	chunk_ids=list(range(nof_chunks))
	
	if os.path.exists(tmp_folder):
		shutil.rmtree(tmp_folder)
	os.mkdir(tmp_folder)

	partial_func=partial(func_meta,file_path,chunksize,cols,cols2)
	
	process=Pool()
	process.map(partial_func,chunk_ids)
	process.close()
	process.join()
	load_meta_df(tmp_folder,'REG_metadata_March_14_2021.pck')

def load_meta_df(tmp_folder,df_saved):
	df_meta=read_pickle(tmp_folder)
	#meta_df=pd.read_pickle('REG_metadata.pck')
	I=df_meta.vending_category_name.isin(['10. Residential','2. T1 Tx FC1 AR STS'])
	df_meta.loc[I,'vending_category_name']='Residential'
	df_meta.loc[~I,'vending_category_name']='Non-Residential'
	df_meta.transaction_date=pd.to_datetime(df_meta.transaction_date)
	df_meta.installation_date=pd.to_datetime(df_meta.installation_date)
	date_threshold=pd.to_datetime('2017-5-01')
	#meta_df=meta_df.explode('vending_category_name')
	#meta_df=meta_df.explode('installation_date')
	#meta_df=meta_df.explode('district_id')
	#meta_df=meta_df.explode('district')
	meta_df=meta_cols(df_meta,date_threshold)
	save_pckle(meta_df,df_saved)

	


if __name__=='__main__':
	file_path='/mnt/nfs/work1/jtaneja/jmugyenyi/REG_consumption_data/reg_data_combined.csv'
	chunksize=1e7
	load_transaction_tmps(file_path,chunksize)		
	load_meta_tmps(file_path,chunksize)
	load_amounts_tmps(file_path,chunksize)
