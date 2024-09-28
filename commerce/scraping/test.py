#!/bin/python3

import math
import os
import random
import re
import sys


#
# Complete the 'finalInstances' function below.
#
# The function is expected to return an INTEGER.
# The function accepts following parameters:
#  1. INTEGER instances
#  2. INTEGER_ARRAY averageUtil
#

def finalInstances(instances, averageUtil):
    # Write your code here


    
    return 0;


if __name__ == '__main__':
    fptr = open(os.environ['OUTPUT_PATH'], 'w')

    instances = int(input().strip())

    averageUtil_count = int(input().strip())

    averageUtil = []

    for _ in range(averageUtil_count):
        averageUtil_item = int(input().strip())
        averageUtil.append(averageUtil_item)

    result = finalInstances(instances, averageUtil)

    fptr.write(str(result) + '\n')

    fptr.close()
