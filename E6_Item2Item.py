import numpy as np
from pyspark import SparkContext
from pyspark import SQLContext
from pyspark.sql import Row
import multiprocessing
from time import time
import math

sc = SparkContext()
Path = "file:/data/hadoop_data/hdfs/"
sqlContext = SQLContext(sc)
MULTI_PROCESS_NUM = 5 # cores / (2~4)



def prepareData(sc):

    print 'import training data'
    #make file to mem
    rawData = sc.textFile(Path + '/UserBehavior/UserBehavior.csv')
    #make RDD
    behavierRDD = rawData.map(lambda line: line.split(','))
    #for quick test

    #make DataFrame
    behaviorRow = behavierRDD.map(lambda p: Row(
                                    userId=int(p[0]),
                                    itemId=p[1],
                                    categaryId=int(p[2]),
                                    behavierType=p[3],
                                    timestrap=int(p[4])))
    # behaviorRow = behaviorRow.take(1000)
    behaviorDF = sqlContext.createDataFrame(behaviorRow)
    behaviorDF.printSchema()
    df = behaviorDF.alias("df")
    return df

def getItemList():
    itemListDF = sqlContext.sql(
        "select distinct(itemId) from user_behavior where behavierType != 'pv'")
    itemList = itemListDF.collect()
    return itemList

def getPurchasedList():
    purchasedListDF = sqlContext.sql(
        "select userId,itemId from user_behavior where behavierType != 'pv'")
    purchasedList = purchasedListDF.collect()
    return purchasedList

def Item2ItemPrepare():
    itemList = getItemList()
    # itemRDD = sc.parallelize(itemList)
    purchasedList = getPurchasedList()
    purchasedRDD = sc.parallelize(purchasedList,2)
    purchasedRow = purchasedRDD.map(lambda p: Row(
                                    userId=int(p[0]),
                                    itemId=p[1]))
    purchaseDF = sqlContext.createDataFrame(purchasedRow)
    purchaseDF.printSchema()
    purchaseDF.registerTempTable('user_purchase')
    return itemList

def ItemSimilarity(userPurchasedItem,itemId,originItem):
    originItemNum = 0
    computedItemNum = 0
    for userId,ItemId in userPurchasedItem:
        if ItemId == itemId:
            computedItemNum += 1
        if ItemId == originItem:
            originItemNum += 1
    similarityVal = computedItemNum / math.sqrt(originItemNum * computedItemNum)
    return similarityVal

def userPurchasedItem(itemId):
    originItem = itemId
    purchasedUsers = sqlContext.sql(
        "select userId from user_purchase where itemId = '"+itemId+"'").collect()
    itemSet = set()
    userIDCondition = ''
    for user in purchasedUsers:
        userIDCondition += "'" + str(user[0]) + "',"
    userPurchasedItem = sqlContext.sql(
        "select userId,itemId from user_purchase where userId in ('"+str(userIDCondition[:-1])+"')").collect()
    for userId,itemId in userPurchasedItem:
        itemSet.add(itemId)
    for item1 in itemSet:
        # compute the ItemSimilarity In SmallSet:
        similarity = ItemSimilarity(userPurchasedItem,itemId,originItem)
        #write to DB use Print instead
        print "from Item: " + originItem + "To Item: " + itemId + " Similarity :" + similarity

def Item2ItemCompute(itemList):
    resultDic = dict()
    pool = multiprocessing.Pool(processes=MULTI_PROCESS_NUM)
    for item in itemList:
        itemId = item[0]
        print itemId + '  add to Compute Queue'
        resultDic[itemId]=(pool.apply_async(userPurchasedItem, (itemId,)))
    pool.close()
    pool.join()

if __name__ == '__main__':
    df = prepareData(sc)
    df.registerTempTable('user_behavior')
    itemList = Item2ItemPrepare()
    Item2ItemCompute(itemList)

