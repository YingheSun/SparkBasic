import numpy as np
from pyspark import SparkContext
from pyspark import SQLContext
from pyspark.sql import Row
from pyspark.mllib.fpm import FPGrowth,FPGrowthModel
from time import time

sc = SparkContext()
Path = "file:/data/hadoop_data/hdfs/"
sqlContext = SQLContext(sc)
MIN_SUPPORT = 0.2
NUM_PARTATION = 10

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

def getUserViews():
    getTime = sqlContext.sql(
        "select min(timestrap) start_time ,max(timestrap) end_time from user_behavior")
    startTime = getTime.first()['start_time']
    endTime = getTime.first()['end_time']

    days = (endTime-startTime)/84600

    # pvRDD = sc.emptyRDD()
    retList = []
    for i in range(days):
        extra_condation = ' and timestrap between ' + str(startTime+i*84600) +' and '+  str(startTime+i*84600+84600) if (startTime+i*84600+84600) < endTime else str(endTime)
        pvDF = sqlContext.sql(
            "select userID,concat_ws(' ',collect_set(itemId)) as user_view from user_behavior where behavierType != 'pv' "+extra_condation+" group by userId")
        if pvDF.count():
            retList += pvDF.rdd.map(lambda p: p['user_view'].split(" ")).collect()

    return [retList]

if __name__ == '__main__':
    df = prepareData(sc)
    df.registerTempTable('user_behavior')
    pvRDD = sc.parallelize(getUserViews(),2)
    pvRDD.persist()
    model = FPGrowth.train(pvRDD,minSupport=MIN_SUPPORT, numPartitions=NUM_PARTATION)
    print model.freqItemsets().collect()
