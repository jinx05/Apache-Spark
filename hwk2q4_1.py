#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Mar 17 17:06:35 2019

@author: jitengirdhar
"""


from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import *

def factor(l):
  return l[0],l[1],l[2][5:len(l[2])-1]

check = 'NY'

#print(userdat_rdd.take(1))
if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("hwk2") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    sc = spark.sparkContext
    
    raw_rdd = sc.textFile("/Users/jitengirdhar/downloads/business.csv")
    rdd1 = raw_rdd.map(lambda x: x.split("::"))
    rddm = rdd1.map(lambda x:list(x))
    rddm1 = rddm.map(factor).filter(lambda x: check in x[1])
    #rddm is the final rdd which has parameters NY in the address string
    #print(rddm1.count())
    buis_sch = "business_id1 full_address categories"
    schema_biz = StructType([StructField(each, StringType(), True) for each in buis_sch.split()])
    df_biz = spark.createDataFrame(rddm1, schema_biz)
    #Adding schema to data
    
    raw_rdd1 = sc.textFile("/Users/jitengirdhar/downloads/review.csv")
    rdd1_n = raw_rdd1.map(lambda x:x.split("::")).map(lambda x:(x[2],x[3])) #only selecting business_id and ratings
    rddrev2 = rdd1_n.mapValues(lambda x:(float(x),1)).reduceByKey(lambda x,y:(x[0] + y[0],x[1] + y[1])) #collecting all rating by business_id and summing all ratings and their number
    rddavg = rddrev2.mapValues(lambda x:x[0]/x[1]).sortBy(lambda x:x[1],ascending=False, numPartitions=1) #average of all ratings and sort by ratings
    
    #rddselfinal = df_sel.rdd.map(list)
    sel_sch = "business_id avg_rating"
    schema_biz = StructType([StructField(each, StringType(), True) for each in sel_sch.split()])
    df_sel1 = spark.createDataFrame(rddavg, schema_biz) #add schema to data
    
    r1 = df_sel1.join(df_biz, df_biz["business_id1"]==df_sel1["business_id"], "cross")
    r2 = r1.sort("avg_rating", ascending=False)
    #selecting top 10 in NY
    h1 = r2.selectExpr("business_id as business_id", "full_address as full_address", "categories as categories", "avg_rating as avg_rating").head(10)
    
    final = sc.parallelize(h1).map(lambda x: str(x[0]) + "\t" + str(x[1]) + "\t" + str(x[2]) + "\t" + str(x[3])) #output format 
    #h1 = r2.select(r2["business_id"], r1["full_address"], r1["categories"], r2["avg_rating"])
    #print(final.take(1))
    dff_final = final.repartition(1).saveAsTextFile("q4")
    sc.stop()

