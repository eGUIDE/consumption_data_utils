import pandas as pd
import os, sys, pickle
import yaml

Data=pd.DataFrame()



#Get the smoothed output file path from the yamlfile
with open('config_cols.yaml') as file_discriptor:
 config=yaml.load(file_discriptor,Loader=yaml.FullLoader)

src=os.path.join(config_cols['batch_files_path'],'smoothed_monthly_data')
dst=config['smoothed_outputfile']

for root,dirs,files in os.walk(src):
    for x in files:
        D=pd.read_pickle(os.path.join(root,x))
        #Data=Data.combine_first(D)
       	Data=pd.concat([Data,D],ignore_index=True)
       
file=open(dst,'wb')
pickle.dump(Data,file)
file.close()
