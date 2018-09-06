import numpy as np
from pyspark.mllib.regression import LabeledPoint
from pyspark import SparkContext
from pyspark.mllib.tree import DecisionTree
from pyspark.mllib.tree import RandomForest
from pyspark.mllib.evaluation import BinaryClassificationMetrics
from time import time
import pandas as pd

sc = SparkContext("local")
impurltyList = ['gini','entropy']
maxDepthList = [3,5,10,15,20,25,30]
# maxDepthList = [3]
maxBinsList = [3,5,10,50,100,200]
# maxBinsList = [3]
treeNumList = [10,20,50,100,1000]
# treeNumList = [10]


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

Path = "file:/data/hadoop_data/hdfs/"

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
    labelPointRDD = lines.map(lambda r:LabeledPoint(extractLabel(r),extractFeatures(r,categoriesMap,len(r)-1)))
    print labelPointRDD.take(10)
    (trainData,testData,validationData) = labelPointRDD.randomSplit([8,1,1])
    print trainData.count()
    print testData.count()
    print validationData.count()
    return (trainData,testData,validationData,categoriesMap)


def predictData(sc, model, categaryMap):
    print 'import test data'
    rawDataWithHeader = sc.textFile(Path + 'train.tsv')
    header = rawDataWithHeader.first()
    rawData = rawDataWithHeader.filter(lambda x: x != header)
    rData = rawData.map(lambda x: x.replace("\"", ""))
    lines = rData.map(lambda x: x.split("\t"))
    dataRDD = lines.map(lambda r:(
        r[0],extractFeatures(r, categaryMap,len(r))
    ))

    descDic = {
        0:'ephemeral',
        1:'evergreen'
    }

    for data in dataRDD.take(1):
        predictResult = model.predict(data[1])
        print descDic[predictResult]

def evaluateModel(model, validationData):
    score = model.predict(validationData.map(lambda p: p.features))
    print score
    scoreAndLabels = score.zip(validationData.map(lambda p: p.label))
    print scoreAndLabels.take(1)
    metrics = BinaryClassificationMetrics(scoreAndLabels)

    return metrics.areaUnderROC

def trainEvaluateModel(trainData, validationData, impurityParam, maxDepthParam, maxBinsParam, treeNum=100):
    startTime = time()
    # model = DecisionTree.trainClassifier(trainData,numClasses=2,categoricalFeaturesInfo={},impurity=impurityParam,maxDepth = maxDepthParam, maxBins = maxBinsParam)
    model = RandomForest.trainClassifier(trainData,numClasses=2,categoricalFeaturesInfo={},numTrees = treeNum ,impurity=impurityParam,maxDepth = maxDepthParam, maxBins = maxBinsParam)
    AUC = evaluateModel(model,validationData)
    durintation = time() - startTime
    print 'durintation' + str(durintation)
    return (AUC, impurityParam, maxDepthParam, maxBinsParam, model)


def evalBestParams(trainData, validationData, impurltyList, maxDepthList, maxBinsList,treeNumList):
    # metrics = [trainEvaluateModel(trainData, validationData, impurity, maxDepth, maxBins) for impurity in impurltyList
    #            for maxDepth in maxDepthList for maxBins in maxBinsList]
    metrics = [trainEvaluateModel(trainData, validationData, impurity, maxDepth, maxBins,treeNum) for impurity in impurltyList
               for maxDepth in maxDepthList for maxBins in maxBinsList for treeNum in treeNumList]

    smetrlcs = sorted(metrics,key=lambda k: k[0],reverse= True)

    df = pd.DataFrame(smetrlcs, columns=["AUC", "impurityParam", "maxDepthParam", "maxBinsParam","model"])
    print df


    bestParam = smetrlcs[0]

    print bestParam
    return bestParam[-1]


if __name__ == '__main__':
    (trainData, testData, validationData,categoriesMap) = prepareData(sc)
    trainData.persist()
    testData.persist()
    validationData.persist()

    # model = DecisionTree.trainClassifier(trainData,2,{},'entropy',5,5)
    # (AUC, impurityParam, maxDepthParam, maxBinsParam, model) = trainEvaluateModel(trainData,validationData,'entropy',5,5)
    # metrics = [trainEvaluateModel(trainData, validationData, impurity, maxDepth, maxBins) for impurity in impurltyList
    #            for maxDepth in maxDepthList for maxBins in maxBinsList]
    # print metrics
    # model = evalBestParams(trainData, validationData, impurltyList, maxDepthList, maxBinsList)
    model = evalBestParams(trainData, validationData, impurltyList, maxDepthList, maxBinsList,treeNumList)
    print model
    #
    # predictData(sc, model, categoriesMap)
    #
    # evaluateModel(model, validationData)