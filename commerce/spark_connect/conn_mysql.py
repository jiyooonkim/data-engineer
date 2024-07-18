# -*- coding: utf-8 -*-
"""
    Connect MySQL
"""
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
import os
 

if __name__ == "__main__":
    spark = SparkSession.builder \
         