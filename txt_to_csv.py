import os
import glob
import pandas as pd
import datetime as dt

#define input directory
dir_in = os.path.join('P:\\','Proj','Poller','iShares_TER')

#define output directory
dir_out = os.path.join('P:\\','Proj','public','Public','iShares_TER_script_output')

#create empty list to store dataframes
df_list = []

#loop through all txt files in input directory.
#for each file, read into dataframe & append dataframe into empty list, df_list
#if error, print out comment & source file, continue
for file in glob.glob(dir_in + '\*.txt'):
	try:
		file_path = os.path.join(dir_in, file)
		df_list.append(pd.read_table(file_path, delimiter='|', skiprows=1))
		continue
	except:
		print('\t Error: text file may have no data to parse. Source: ' + file_path)

#concatenate list of dataframes into one large dataframe
df = pd.concat(df_list)

#define today's date in mm/dd/yy format
today = dt.datetime.today().strftime("_%m-%d-%Y")

#change to output directory
os.chdir(dir_out)

#save large dataframe as csv
df.to_csv('script_output' + today + '.csv')