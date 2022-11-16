#!/usr/bin/python3
import pickle
import numpy as np
from operator import itemgetter
import os
import time

"""
This benchmark represents the "Serverless GEMM" benchmark as implemented in numpywren framework[1]

Matrix multiple is implemented using map-reduce following the Cannon’s Algorithm [3]
A split function is used to shard the input into chunks for the mapper and reducer.
The split function for the reducer
All input and output data for the functions are maintained as independent objects.

[2] Is the basis of this implementation but its use of bash sort is incorrect when using indices greater than 10
since it does a lexicographic sort than numeric sort

[1] https://github.com/Vaishaal/numpywren
[2] https://gist.github.com/gpavanb1/fe319f6929b0c0938d45898e09d117a6#file-serverless-gemm-ipynb
[3] https://github.com/amberm291/MatrixMultiplyMR
"""
################## SPLIT INPUT FOR MAP OR REDUCE WORKERS
def split(event_message, split_at):
    ### get begin
    matrix = pickle.loads(event_message)
    ### get end
    
    ### compute begin
    start = time.time()
    # split before reduce requires sorting
    matrix_split = np.split(matrix, split_at)
    #print(time.time() - start)
    ### compute end
    
    ### put begin
    pickle.dumps(matrix_split)
    #os.remove('p.pickle')
    #with open('p.pickle','wb') as h:
    #    pickle.dump(matrix_split, h, protocol=pickle.DEFAULT_PROTOCOL)
    #exit()
    ### put end
    
    return matrix_split
    
################## MAPPER
def mapper(event_message, dimensions):
    ### get begin
    matrix = pickle.loads(event_message)    
    ### get end

    ### compute begin
    start = time.time()
    row_a, col_b = map(int,dimensions)
    out = []
    for line in matrix:
        matrix_index, row, col, value = line.rstrip().split(",")
        if matrix_index == "A":
            for i in range(0,col_b):
                key = row + "," + str(i)
                out.append("%s\t%s\t%s"%(key,col,value))
        else:
            for j in range(0,row_a):
                key = str(j) + "," + col
                out.append("%s\t%s\t%s"%(key,row,value))
    #print(time.time() - start)
    ### compute end
    
    ### put begin
    pickle.dumps(out)
    ### put end
    
    return out

################## REDUCER
def reducer(event_message):
    ### get begin
    matrix = pickle.loads(event_message)
    ### get end
        
    ### compute begin
    start = time.time()
    prev_index = None
    value_list = []
    out = []

    for line in matrix:
        curr_index, index, value = line.rstrip().split("\t")
        index, value = map(int,[index,value])
        if curr_index == prev_index:
            value_list.append((index,value))
        else:
            if prev_index:
                value_list = sorted(value_list,key=itemgetter(0))
                i = 0
                result = 0
                while i < len(value_list) - 1:
                    if value_list[i][0] == value_list[i + 1][0]:
                        result += value_list[i][1]*value_list[i + 1][1]
                        i += 2
                    else:
                        i += 1
                out.append("%s,%s"%(prev_index,str(result)))
            prev_index = curr_index
            value_list = [(index,value)]

    if curr_index == prev_index:
        value_list = sorted(value_list,key=itemgetter(0))
        i = 0
        result = 0
        while i < len(value_list) - 1:
            if value_list[i][0] == value_list[i + 1][0]:
                result += value_list[i][1]*value_list[i + 1][1]
                i += 2
            else:
                i += 1
        out.append("%s,%s"%(prev_index,str(result)))
    print(time.time() - start)
    ### compute end
    
    ### put begin
    pickle.dumps(out)
    ### put end
    
    return out
    

################## MAIN

### init
# NOTE: the below assumes 2 shards of input i.e., 2 map and 2 reduce processes
# for different num_segments, correctly populate the split_map split_reduce array
# split_reduce should be broken such that curr_index == prev_index inside each segment

# for input.txt 5x5 matrices
#matrix_path = "data/input.txt"
#dimensions = [5,5]
#split_map = [21]
#split_reduce = [101]

# for input-medium.txt 200x200 matrices
matrix_path = "data/input-medium.txt"
dimensions = [200,200]
split_map = [9954]
split_reduce = [1990855]

# for input-large.txt 500x500 matrices
#matrix_path = "data/input-large.txt"
#dimensions = [500,500]
#split_map = [62263]
#split_reduce = []



matrix = open(matrix_path).readlines()

### func1 - split
event_message = pickle.dumps(matrix)
matrix_split = split(event_message, split_map)

### func2 (multiple mapper instances can be called in parallel)
mapper_out = []
for ms in matrix_split:
    event_message = pickle.dumps(ms)
    mapper_out.extend(mapper(event_message, dimensions))

### This sort and shuffle is assumed to be done by the MapReduce framework, outside of this FaaS application
### Refer - https://techvidvan.com/tutorials/hadoop-mapreduce-shuffling-and-sorting/
mapper_out.sort( key=lambda x: (int(x.split("\t")[0].split(",")[0]), int(x.split("\t")[0].split(",")[1])) )

### func3 - split
event_message = pickle.dumps(mapper_out)
matrix_split1 = split(event_message, split_reduce)

### func4 (multiple reducer instances can be called in parallel)
result = []
for ri in matrix_split1:
    event_message = pickle.dumps(ri)
    result.extend(reducer(event_message))
    
#print("\n".join(result))



