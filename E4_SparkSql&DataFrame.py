import numpy as np
from pyspark import SparkContext
from pyspark import SQLContext
from pyspark.sql import Row
from time import time

sc = SparkContext()
Path = "file:/data/hadoop_data/hdfs/"
sqlContext = SQLContext(sc)

def prepareData(sc):

    print 'import training data'
    #make file to mem
    rawData = sc.textFile(Path + '/UserBehavior/UserBehavior.csv')
    print rawData.first()
    #make RDD
    behavierRDD = rawData.map(lambda line: line.split(','))
    print behavierRDD.take(3)
    #for quick test

    #make DataFrame
    behaviorRow = behavierRDD.map(lambda p: Row(
                                    userId=int(p[0]),
                                    itemId=int(p[1]),
                                    categaryId=int(p[2]),
                                    behavierType=p[3],
                                    timestrap=int(p[4])))
    print behaviorRow.take(3)
    # behaviorRow = behaviorRow.take(100000)
    behaviorDF = sqlContext.createDataFrame(behaviorRow)
    behaviorDF.printSchema()
    df = behaviorDF.alias("df")
    return df

def getTableCounts():
    totalCount = sqlContext.sql("select count(*) row_counts from user_behavior").first()
    userCount = sqlContext.sql("select count(distinct userId) user_counts from user_behavior").first()
    itemCount = sqlContext.sql("select count(distinct itemId) item_counts from user_behavior").first()
    itemList = sqlContext.sql("select distinct itemId from user_behavior").collect()
    print "totalCount lines in table:" +str(totalCount['row_counts'])
    print "totalCount users in table:" +str(userCount['user_counts'])
    print "totalCount items in table:" +str(itemCount['item_counts'])
    return (totalCount['row_counts'],userCount['user_counts'],itemCount['item_counts'],itemList)


def getUserBehaviers():
    pvDF = sqlContext.sql(
        "select userId,count(itemId) user_view from user_behavior where behavierType = 'pv' group by userId").toDF('userId','1')
    buyDF = sqlContext.sql(
        "select userId,count(itemId) user_buy from user_behavior where behavierType = 'buy' group by userId").toDF('userId','2')
    cartDF = sqlContext.sql(
        "select userId,count(itemId) user_cart from user_behavior where behavierType = 'cart' group by userId").toDF('userId','3')
    favDF = sqlContext.sql(
        "select userId,count(itemId) user_fav from user_behavior where behavierType = 'fav' group by userId").toDF('userId','4')
    userDF = pvDF.join(buyDF).join(cartDF).join(favDF)
    print userDF.printSchema()
    print userDF.show()

def makeOneStepItemMarkovMatrix(itemList):
    trace = len(itemList)
    retMat = np.zeros(trace)
    itemBuyList = []
    buyCounter = 0
    itemReCheckList = []
    checkCounter = 0
    checkItemNumber = 0
    for itemRaw in itemList:
        queryBuyDF = "select userId,timestrap from user_behavior where behavierType = 'buy' and itemId = '"+str(itemRaw['itemId'])+"'"
        buyDF = sqlContext.sql(queryBuyDF)
        buyCount = buyDF.count()
        if buyCount:
            buyPD = buyDF.toPandas()
            print buyPD
            re_check_count = 0
            for indexs in buyPD.index:
                userId = str(buyPD.loc[indexs].values[0])
                startTimestramp = str(buyPD.loc[indexs].values[1]+1)
                endTimestramp = str(buyPD.loc[indexs].values[1]+7*86400*4)
                # use the same item to instead the same cluster products
                re_check_time = sqlContext.sql("select count(itemId) as re_check_flag from user_behavior where itemId = '"+str(itemRaw['itemId'])+"' and userId = '"+userId+"' and timestrap between '"+startTimestramp+"' and '"+endTimestramp+"'")
                re_check_time = re_check_time.first()
                re_check_count += re_check_time['re_check_flag']
            print re_check_count
            itemBuyList.append(buyCount)
            buyCounter += buyCount
            itemReCheckList.append(re_check_count)
            checkCounter += re_check_count
            checkItemNumber += 1
        else:
            itemBuyList.append(0)
            itemReCheckList.append(0)

    medianCheckNumber = checkCounter / float(checkItemNumber) + 0.001
    print medianCheckNumber
    for i in range(trace):
        distance = np.abs(itemReCheckList[i] - medianCheckNumber)
        if itemBuyList[i]:
            print i
            print distance
            retMat[i] = itemBuyList[i] / float(buyCounter) * distance
            norm = np.linalg.norm(retMat)
    print retMat/norm


if __name__ == '__main__':
    df = prepareData(sc)
    df.registerTempTable('user_behavior')
    df.cache()
    (row_counts,user_counts,item_counts,itemList) = getTableCounts()
    getUserBehaviers()
    # can use batched result to run
    makeOneStepItemMarkovMatrix(itemList[500:600])



