import os

mypath = '/mnt/nfs/eguide/projects/electricity_prediction/data/REG_data/smoothing_process/batch_grouped_consumer_ids'
f= open('batch_process_smoothing.sh','w')
f.write('#!/bin/bash\n')
f.write('\n')

for filename in  os.listdir(mypath):
    f.write('sbatch batch_smoothing.sh "{}" '.format(filename))
    f.write('\n')
f.close()
