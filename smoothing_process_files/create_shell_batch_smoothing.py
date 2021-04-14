import os
import yaml
import subprocess

with open('config_cols.yaml') as file_discriptor:
 config_cols=yaml.load(file_discriptor,Loader=yaml.FullLoader)
batch_files_path=config_cols['batch_files_path']

mypath = os.path.join(batch_files_path,'batch_grouped_consumer_ids')
file_name='batch_process_smoothing.sh'

if os.path.exists(file_name):
	os.remove(file_name)

f= open(file_name,'w')
f.write('#!/bin/bash\n')
f.write('\n')

for filename in  os.listdir(mypath):
    f.write('sbatch batch_smoothing.sh "{}" '.format(filename))
    f.write('\n')
f.close()


subprocess.call('batch_process_smoothing.sh')
