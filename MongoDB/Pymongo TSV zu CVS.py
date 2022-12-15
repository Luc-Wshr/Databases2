# TSV zu CSV

import pandas as pd
 
tsv_file='name.basics.tsv'
 
# reading given tsv file
csv_table=pd.read_table(tsv_file,sep='\t', low_memory=False)
 
# converting tsv file into csv
csv_table.to_csv('name.basics.csv',index=False)
 
# output
print("Successfully made csv file")
