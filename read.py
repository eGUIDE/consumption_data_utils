import pandas as pd
import os
import pickle
import sys

Data=pd.DataFrame()

'path is the name of the directory containing files to be merged'
path=sys.argv[1]

"path2 is the name of the new file we want to create"
path2=sys.argv[2]
for root,dirs,files in os.walk(path):
    for x in files:
        D=pd.read_pickle(os.path.join(root,x))
        #Data=Data.combine_first(D)
       	Data=pd.concat([Data,D],ignore_index=True)
       
file=open(path2,'wb')
pickle.dump(Data,file)
file.close()

