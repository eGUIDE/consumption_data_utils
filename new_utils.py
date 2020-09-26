import pandas as pd
import os,pickle,shutil
import numpy as np
from functools import partial
from multiprocessing import Pool
import time



def save_pckle(pop,ppk):
'''
The function saves pop as a pickle named ppk in present directory
'''
        file=open(ppk,'wb')
        pickle.dump(pop,file)
        file.close()


def read_pickle(tmp_dir,output_file):
'''
The function reads all the pickle files in tmp_dir and concatnates them into a single pickle file called output_file
'''
	Data=pd.DataFrame()
	path=tmp_dir
	for root,dirs,files in os.walk(path):
    		for x in files:
        		D=pd.read_pickle(os.path.join(root,x))
        		#Data=Data.combine_first(D)
       			Data=pd.concat([Data,D],ignore_index=True)
    
	file=open(output_file,'wb')
	pickle.dump(Data,file)
	file.close()

def meta_cols(df):
	meta_obj=df.groupby(['consumer_id','meter_serial_number'])
	meta_df=pd.DataFrame(data={'installation_date':meta_obj.installation_date.min()})
	meta_df['vending_category_name']=meta_obj.vending_category_name.unique()
	meta_df['district_id']=meta_obj.district_id.unique()
	meta_df['district']=meta_obj.district.unique()
	return meta_df.reset_index()


def func_meta(file_path,chunksize,cols,ids):
'''
This function writes the chunks for the meta data file
'''
	try:
		if ids==0:
			df=pd.read_csv(file_path,nrows=chunksize)
		else:
			df=pd.read_csv(file_path,nrows=chunksize,skiprows=int(chunksize*ids))
			df.columns=cols
			
		meta_df=meta_cols(df)
		save_pckle(meta_df,'tmp_meta_dir/meta_chunk_%s.pck'%str(ids))		
	except Exception as err:
		print(err)

def func(file_path,chunksize,cols,ids):
'''
This function writes the chunks for the transactions data file
'''
	try:
		if ids==0:
			df=pd.read_csv(file_path,nrows=chunksize)
		else:
			df=pd.read_csv(file_path,nrows=chunksize,skiprows=int(chunksize*ids))
			df.columns=cols
		
		df2=df[['consumer_id','meter_serial_number','transaction_date','tariff_name','kWh_sold']]
		save_pckle(df2,'tmp_dir/chunk_%s.pck'%str(ids))		
	except Exception as err:
		print(err)
		

def load_transaction_data(file_path,chunksize):
'''
The function loads transaction data using multiprocess
'''
	tmp_folder='tmp_dir'
	df=pd.read_csv(file_path,nrows=chunksize)
	cols=df.columns
	nof_chunks=int(os.path.getsize(file_path)/(df.memory_usage(index=True).sum()))
	chunk_ids=list(range(nof_chunks))
	
	if os.path.exists(tmp_folder):
		shutil.rmtree(tmp_folder)
	os.mkdir(tmp_folder)

	partial_func=partial(func,file_path,chunksize,cols)
	
	process=Pool()
	process.map(partial_func,chunk_ids)
	process.close()
	process.join()

	read_pickle(tmp_folder,'chuncks.pck')
#	shutil.rmtree(tmp_folder)


def load_meta(file_path,chunksize):
'''
The function loads meta data file using multiprocess
'''
	tmp_folder='tmp_meta_dir'
	df=pd.read_csv(file_path,nrows=chunksize)
	cols=df.columns
	nof_chunks=int(os.path.getsize(file_path)/(df.memory_usage(index=True).sum()))
	chunk_ids=list(range(nof_chunks))
	
	if os.path.exists(tmp_folder):
		shutil.rmtree(tmp_folder)
	os.mkdir(tmp_folder)

	partial_func=partial(func_meta,file_path,chunksize,cols)
	
	process=Pool()
	process.map(partial_func,chunk_ids)
	process.close()
	process.join()

	read_pickle(tmp_folder,'meta_chuncks.pck')
	meta_df=pd.read_pickle('meta_chuncks.pck')
	meta_df=meta_df.explode('vending_category_name')
	meta_df=meta_df.explode('district_id')
	meta_df=meta_df.explode('district')
	meta_df=meta_cols(meta_df)
	save_pckle(meta_df,'meta_chuncks.pck')

if __name__=='__main__':
	file_path='/mnt/nfs/work1/jtaneja/jmugyenyi/REG_consumption_data/reg_data_combined.csv'
	chunksize=1e7
	load_transaction_data(file_path,chunksize)		
	load_meta(file_path,chunksize)
