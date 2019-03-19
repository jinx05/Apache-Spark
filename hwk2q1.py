#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jan 27 20:31:37 2019

@author: jitengirdhar
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import *


def unpack(x):
  lt = []
  for i in x[1].split(","):
    if( x[0] > i):
      lt.append((i + "," + x[0],x[1].split(",")))
    else:
      lt.append((x[0] + "," + i,x[1].split(",")))
  return lt


if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("hwk2") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    sc = spark.sparkContext
    #split the lines of each data frame by /t
    rdd_input = sc.textFile("/Users/jitengirdhar/downloads/soc-LiveJournal1Adj.txt").map(lambda x: x.split("\t"))
    #unpack operation on each line gives a key value pair for each line
    rdd3 = rdd_input.flatMap(unpack) 
    #reduce by key operation and find the common friends
    rdd_mapped = rdd3.reduceByKey(lambda x,y: list(set(x) - (set(x) - set(y))))
    #as per the question statement we output only those friend pairs which have some mutual friends
    rdd5 = rdd_mapped.filter(lambda x: len(x[1]) > 0).map(lambda x: (x[0].split(",")[0],x[0].split(",")[1],len(x[1]))).map(lambda x: str(x[0]) + "," + str(x[1]) + "\t" + str(x[2]))

    df_result = rdd5.coalesce(1).saveAsTextFile("q1")
    sc.stop()

   