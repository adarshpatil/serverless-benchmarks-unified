from scipy.sparse import random
from scipy import stats
from numpy.random import default_rng
import numpy as np

'''
matrixA = np.random.randint(0,50,(500,500))
matrixB = np.random.randint(0,50,(500,500))

'''

rng = default_rng()

matrixA = random(200, 200, dtype=np.uint8, density=0.25)
matrixB = random(200, 200, dtype=np.uint8, density=0.25)

for index, v in np.ndenumerate(matrixA.todense()):
    if v!=0:
        print('A,{},{},{}'.format(index[0], index[1], v))

for index, v in np.ndenumerate(matrixB.todense()):
    if v!=0:
        print('B,{},{},{}'.format(index[0], index[1], v))
