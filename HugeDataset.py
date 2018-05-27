#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Sun May 27 10:25:09 2018

@author: eduardo
"""

#https://datascienceplus.com/processing-huge-dataset-with-python/

# Load the required packages
import time
import psutil
import numpy as np
import pandas as pd
import multiprocessing as mp

# Check the number of cores and memory usage
num_cores = mp.cpu_count()
print "This kernel has ",num_cores,"cores and you can find the information regarding the memory usage:",psutil.virtual_memory()

# Writing as a function
def process_user_log(chunk):
    grouped_object = chunk.groupby(chunk.index,sort = False) # not sorting results in a minor speedup
    func = {'date':['min','max','count'],
            'num_25':['sum'],
            'num_50':['sum'], 
            'num_75':['sum'],
            'num_985':['sum'],
            'num_100':['sum'],
            'num_unq':['sum'],
            'total_secs':['sum']}
    answer = grouped_object.agg(func)
    return answer
    
# Number of rows for each chunk
size = 2e7 # 20 Millions
reader = pd.read_csv('user_logs.csv', chunksize = size, index_col = ['msno'])
start_time = time.time()

for i in range(20):
    user_log_chunk = next(reader)
    if(i==0):
        result = process_user_log(user_log_chunk)
        print("Number of rows ",result.shape[0])
        print("Loop ",i,"took %s seconds" % (time.time() - start_time))
    else:
        result = result.append(process_user_log(user_log_chunk))
        print("Number of rows ",result.shape[0])
        print("Loop ",i,"took %s seconds" % (time.time() - start_time))
    del(user_log_chunk)    

# Unique users vs Number of rows after the first computation    
print("size of result:", len(result))
check = result.index.unique()
print("unique user in result:", len(check))

result.columns = ['_'.join(col).strip() for col in result.columns.values]
                  
'''
    Second pass, the DF is smaller than the original
'''
                  
func = {'date_min':['min'],
        'date_max':['max'],
        'date_count':['count'] ,
        'num_25_sum':['sum'],
        'num_50_sum':['sum'],
        'num_75_sum':['sum'],
        'num_985_sum':['sum'],
        'num_100_sum':['sum'],
        'num_unq_sum':['sum'],
        'total_secs_sum':['sum']}

processed_user_log = result.groupby(result.index).agg(func)
print(len(processed_user_log))

'''
    Final aggregated dataset
'''
processed_user_log.columns = processed_user_log.columns.get_level_values(0)
processed_user_log.head()

