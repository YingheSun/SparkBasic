import numpy as np
from time import time
import pandas as pd
from pyspark.mllib.regression import LabeledPoint
from pyspark import SparkContext
from pyspark.mllib.evaluation import BinaryClassificationMetrics
from pyspark.mllib.classification import LogisticRegressionWithLBFGS
from pyspark.mllib.classification import LogisticRegressionWithSGD
from pyspark.mllib.feature import StandardScaler

sc = SparkContext()
Path = "file:/data/hadoop_data/hdfs/"
numInterationList = [3,5,10,15,20]
stepSizeList = [10,50,100,30,40]
miniBatchFactionList = [0.1,0.2,0.3,0.5,0.8,1.0]

def convert_float(x):
    return 0 if x == '?' else float(x)


def extractFeatures(field, categariesMap, featureEnd):
    categaryIdx = categariesMap[field[3]]
    categaryFeatures = np.zeros(len(categariesMap))
    categaryFeatures[categaryIdx] = 1

    numericalFeatures = [convert_float(field) for field in field[4:featureEnd]]
    return np.concatenate((categaryFeatures,numericalFeatures))

def extractLabel(field):
    return float(field[-1])

def prepareData(sc):

    print 'import training data'

    rawDataWithHeader = sc.textFile(Path + 'train.tsv')
    print rawDataWithHeader.take(10)
    header = rawDataWithHeader.first()
    rawData = rawDataWithHeader.filter(lambda x:x != header)
    rData = rawData.map(lambda x: x.replace("\"",""))
    lines = rData.map(lambda x: x.split("\t"))
    print lines.count()

    categoriesMap = lines.map(lambda fields:fields[3]).distinct().zipWithIndex().collectAsMap()
    print categoriesMap
    labelRDD = lines.map(lambda r: extractLabel(r))
    featureRDD = lines.map(lambda r: extractFeatures(r,categoriesMap,len(r)-1))
    # print featureRDD.take(1)
    stdScaler = StandardScaler(withMean=True,withStd=True).fit(featureRDD)
    ScalerFeatureRDD = stdScaler.transform(featureRDD)
    # print ScalerFeatureRDD.take(1)
    labelPoint = labelRDD.zip(ScalerFeatureRDD)
    labelPointRDD = labelPoint.map(lambda r: LabeledPoint(r[0],r[1]))
    # print labelPointRDD.take(1)
    (trainData, testData, validationData) = labelPointRDD.randomSplit([8, 1, 1])
    print trainData.count()
    print testData.count()
    print validationData.count()
    return (trainData, testData, validationData, categoriesMap)

def evaluateModel(model, validationData):
    score = model.predict(validationData.map(lambda p: p.features))
    print score
    scoreAndLabels = score.zip(validationData.map(lambda p: p.label)).map(lambda (x,y): (float(x),float(y)))
    print scoreAndLabels.take(1)
    metrics = BinaryClassificationMetrics(scoreAndLabels)

    return metrics.areaUnderROC

def trainEvaluateModel(trainData, validationData, numInterations, stepSize, minibatchFaction):
    startTime = time()

    model = LogisticRegressionWithSGD.train(trainData,numInterations,stepSize,minibatchFaction)
    # model = LogisticRegressionWithSGD.train(trainData)
    # model = LogisticRegressionWithLBFGS(trainData,numInterations,stepSize,minibatchFaction)
    AUC = evaluateModel(model,validationData)
    durintation = time() - startTime
    print 'durintation' + str(durintation)
    return (AUC, numInterations, stepSize, minibatchFaction, model)

def evalBestParams(trainData, validationData, numInterationsList, stepSizeList, minibatchFactionList):
    metrics = [trainEvaluateModel(trainData, validationData, numInterations, stepSize, minibatchFaction) for numInterations in numInterationsList
               for stepSize in stepSizeList for minibatchFaction in minibatchFactionList]

    smetrlcs = sorted(metrics,key=lambda k: k[0],reverse= True)

    df = pd.DataFrame(smetrlcs, columns=["AUC", "numInterations", "stepSize", "minibatchFaction","model"])
    print df


    bestParam = smetrlcs[0]

    print bestParam
    return bestParam[-1]



if __name__ == '__main__':
    (trainData, testData, validationData, categoriesMap)=prepareData(sc)
    trainData.persist()
    testData.persist()
    validationData.persist()
    print testData
    print evalBestParams(trainData, validationData, numInterationList, stepSizeList, miniBatchFactionList)
    # model = LogisticRegressionWithSGD.train(trainData)
    # print model
    # print model.predict(validationData.map(lambda p: p.features))