#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Mar 17 14:20:00 2019

@author: jitengirdhar
"""

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import *

def factor(l):
  return l[0],l[1],l[2][5:len(l[2])-1]

check = 'Colleges & Universities'

#print(userdat_rdd.take(1))
if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("hwk2") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    sc = spark.sparkContext
    
    #df1 = spark.read.format("csv").option("header", "true").load("/FileStore/tables/business.csv")
    raw_rdd = sc.textFile("/Users/jitengirdhar/downloads/business.csv")
    rdd1 = raw_rdd.map(lambda x: x.split("::"))
    rddm = rdd1.map(lambda x:list(x))
    rddm1 = rddm.map(factor).filter(lambda x: check in x[2])
    #print(rddm1.count())
    buis_sch = "business_id full_address categories"
    schema_biz = StructType([StructField(each, StringType(), True) for each in buis_sch.split()])
    df_biz = spark.createDataFrame(rddm1, schema_biz)
    #print(df_biz.take(2))
    raw_rdd1 = sc.textFile("/Users/jitengirdhar/downloads/review.csv")
    rdd1_n = raw_rdd1.map(lambda x:x.split("::")).map(lambda x:(x[0],x[1],x[2],"\t" + str(x[3])))
    review_sch = "review_id user_id business_id stars"
    schema_rev = StructType([StructField(each,StringType(),True) for each in review_sch.split()])
    df_rev = spark.createDataFrame(rdd1_n,schema_rev)
    
    #print(df_rev.count())
    r1 = df_rev.join(df_biz, df_biz["business_id"]==df_rev["business_id"], "cross")
    h1 = r1.selectExpr("user_id as user_id", "stars as stars").distinct()
    rddfinal = h1.rdd.map(tuple).map(lambda x: str(x[0]) + "\t" + str(x[1]))
    dff_final = rddfinal.repartition(1).saveAsTextFile("q3_final")
    sc.stop()
    #print(h1.count())