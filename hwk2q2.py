#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Mar 17 02:04:04 2019

@author: jitengirdhar
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *


def unpack(x):
  lt = []
  for i in x[1].split(","):
    if( x[0] > i):
      lt.append((i + "," + x[0],x[1].split(",")))
    else:
      lt.append((x[0] + "," + i,x[1].split(",")))
  return lt

def reqinfo(x):
  u = x.split(",")
  return u[0],u[1],u[2],u[3]

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("hwk2") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    sc = spark.sparkContext
    rdd_input = sc.textFile("/Users/jitengirdhar/downloads/soc-LiveJournal1Adj.txt").map(lambda x: x.split("\t"))
    #generating key value pairs from each line
    rdd3 = rdd_input.flatMap(unpack)
    #reduce by key operation to get the list which contains the userid of common friends
    rdd_mapped = rdd3.reduceByKey(lambda x,y: list(set(x) - (set(x) - set(y))))
    #as we need only those user pairs with mutual friends and split the data because we want to output as given in the question
    rdd5 = rdd_mapped.filter(lambda x: len(x[1]) > 0).map(lambda x: (x[0].split(",")[0],x[0].split(",")[1],len(x[1])))
    df_rdd = spark.createDataFrame(rdd5)
    df_sel = df_rdd.select("_1", "_2", "_3").sort("_3", ascending=False).head(10)  #selection in dataframes and sort in descending order of total friends

    sel_sch = "user1 user2 total"
    schema_sel = StructType([StructField(each, StringType(), True) for each in sel_sch.split()]) #Assign schema to get a dataframe with top 10 mutual friends
    df_top10 = spark.createDataFrame(sc.parallelize(df_sel), schema_sel)
    
    userdat_rdd = sc.textFile("/Users/jitengirdhar/downloads/userdata.txt") #user details data set
    
    userdel_rdd = userdat_rdd.map(reqinfo)
    #print(userdel_rdd.take(2))

    user_sch = "user fname lname address"
    schema_user = StructType([StructField(each, StringType(), True) for each in user_sch.split()])
    df_user = spark.createDataFrame(userdel_rdd, schema_user)
    
    #we can apply joins on dataframes to get our required results, here we join twice
    r1 = df_top10.join(df_user, df_user["user"]==df_top10["user1"], "cross")
    h1 = r1.selectExpr("total as total", "user as user", "fname as fname1","lname as lname1", "address as address1","user2 as user2")
    r2 = h1.join(df_user,df_user["user"] == h1["user2"], "cross")

    h2 = r2.select(h1["total"], h1["fname1"],h1["lname1"],h1["address1"],df_user["fname"], df_user["lname"], df_user["address"]) #selecting what values to show according to the question statement
    #print(h2.take(10))
    final = h2.rdd.map(tuple).map(lambda x: str(x[0]) + "\t" + str(x[1]) + "\t" + str(x[2]) + "\t" + str(x[3]) + "\t" + str(x[4]) + "\t" + str(x[5]) + "\t" + str(x[6]))
    dff_final = final.repartition(1).saveAsTextFile("q2_final1")
    sc.stop()
