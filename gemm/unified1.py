import pickle
import numpy as np
from operator import itemgetter

def split(event_message, num_segments):
    ### get begin
    matrix = pickle.loads(event_message)
    ### get end
    
    ### compute begin
    matrix.sort()
    matrix_split = np.array_split(matrix, num_segments)
    ### compute end
    
    ### put begin
    pickle.dumps(matrix_split)
    ### put end
    
    return matrix_split
    
################## MAPPER
def mapper(event_message, dimensions):
    ### get begin
    matrix = pickle.loads(event_message)    
    ### get end

    ### compute begin
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
    ### compute end
    
    ### put begin
    pickle.dumps(out)
    ### put end
    
    return out
    

################## MAIN

### init
matrix_path = "data/input.txt"
num_segments = 2
dimensions = [5,5]
matrix = open(matrix_path)


### func1 - split
event_message = pickle.dumps(matrix.readlines())
matrix_split = split(event_message, num_segments)

### func2 (multiple mapper instances can be called in parallel)
mapper_out = []
for ms in matrix_split:
    event_message = pickle.dumps(ms)
    mapper_out.extend(mapper(event_message, dimensions))

### func3 - split
event_message = pickle.dumps(mapper_out)
matrix_split1 = split(event_message, num_segments)

### func4 (multiple reducer instances can be called in parallel)
result = []
for ri in matrix_split1:
    event_message = pickle.dumps(ri)
    result.extend(reducer(event_message))
    
print(len(result))
print(result)



