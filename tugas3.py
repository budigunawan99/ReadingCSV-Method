#!/usr/bin/env python3

"""
@author: bnawan
"""

import pandas as pd
import time
import csv
import dask.dataframe as dsk
import collections
from matplotlib import pyplot as plt

dataset = "Movies_and_TV.csv"
label = ["asin","reviewerID","overall"]
score={}

######################### pandas with chunk ###################################
start_time = time.time()
chunksize= 10 ** 6
# print(chunksize)

chunk_list=[]

for data in pd.read_csv(dataset, delimiter=',',names=label, chunksize=chunksize): 
    chunk_list.append(data)
    
filtered_data = pd.concat(chunk_list)
print(filtered_data.head(5))
score["pandaswithchunk"]=time.time() - start_time
print("--- %s seconds ---" % (time.time() - start_time))

######################### pandas without chunk ###################################

start_time = time.time()

data = pd.read_csv(dataset, delimiter=',',names=label)
print(data.head(5))
score["pandaswithoutchunk"]=time.time() - start_time
print("--- %s seconds ---" % (time.time() - start_time))     

######################### CSV Reader ###################################

start_time = time.time()
print("\tasin\treviewerID\toverall")
with open("Movies_and_TV.csv") as csvfile:  
    data = csv.reader(csvfile, delimiter=",")
    count = 0
    for row in data:
        if count < 5:
            print(row[0]+" "+row[1]+" "+row[2]+"\t"+row[3])
        else:
            break
        count+=1
score["CSVReader"]=time.time() - start_time
print("--- %s seconds ---" % (time.time() - start_time))

######################### Dask Dataframe ###################################

start_time = time.time()

df = dsk.read_csv(dataset,names=label,dtype='str')
print(df.head(5))
score["DaskDataframe"]=time.time() - start_time
print("--- %s seconds ---" % (time.time() - start_time))

######################### Plotting ###################################

method = []
value = []

sortedmethod = sorted(score.items(), key=lambda kv: kv[1], reverse=True)
sorteddict = collections.OrderedDict(sortedmethod)

for data in sorteddict.keys():
    method.append(data)
    value.append(score[data])

print(method)
print(value)

xs = [i + 0.1 for i, _ in enumerate(method)]
# plot bars with left x-coordinates [xs], heights [num_oscars]
plt.bar(xs, value, color='g')
plt.xlabel("method")
plt.ylabel("time(s)")
plt.title("Top 4 Method for Reading CSV")
# label x-axis with movie names at bar centers
plt.xticks([i + 0.1 for i, _ in enumerate(method)], method)
plt.show()