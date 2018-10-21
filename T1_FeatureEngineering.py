import numpy as np
from pyspark import SparkContext
from pyspark import SQLContext
from pyspark.sql import Row
import multiprocessing
import time
import math
# from T1_Classes import Spline
# from T1_Classes import User
import json

sc = SparkContext()
Path = "file:/data/hadoop_data/hdfs/"
sqlContext = SQLContext(sc)

#parameters
SPLINE_LIMIT = int(60*20)

class Spline(object):

    def __init__(self,firstAction):

        #starting
        self.user_id = firstAction['userId']
        self.start_timestramp = firstAction['timestrap']
        self.struct_time = time.localtime(self.start_timestramp)
        self.day_of_week = self.struct_time.tm_wday
        self.time_of_day = self.struct_time.tm_hour

        #loop action exchange
        self.spline_list = []
        self.spline_pv_count = 0
        self.spline_buy_count = 0
        self.spline_fav_count = 0
        self.spline_cart_count = 0
        self.steps = 0
        self.is_buyed = 0
        self.is_faved = 0
        self.is_carted = 0

        # for end
        self.end_timrstramp = 0
        self.duration = 0

        print 'user :: ' + str(self.user_id) + '  spline >>>' + str(self.start_timestramp)

    def newAction(self,action):
        action_type = action['behavierType']
        self.steps += 1
        self.spline_list.append(action)
        self.end_timrstramp = action['timestrap']
        self.duration = action['timestrap'] - self.start_timestramp
        if action_type == 'pv':
            self.spline_pv_count += 1
        elif action_type == 'buy':
            self.spline_buy_count += 1
            self.is_buyed = 1
        elif action_type == 'cart':
            self.spline_cart_count += 1
            self.is_carted = 1
        elif action_type == 'fav':
            self.spline_fav_count += 1
            self.is_faved = 1


def prepareData(sc):

    print '>>>>>>>>import training data'
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

def get_user_list():
    print '>>>>>>>>user distinct list'
    userList = sqlContext.sql("select distinct userId from user_behavior").collect()
    return userList

def get_user_action_list(user_id):
    print '>>>>>>>>each user action list'
    userActionList = sqlContext.sql("select * from user_behavior where userId = '" + str(user_id) + "' order by timestrap asc").collect()
    return userActionList

def getTableCounts():
    totalCount = sqlContext.sql("select count(*) row_counts from user_behavior").first()
    userCount = sqlContext.sql("select count(distinct userId) user_counts from user_behavior").first()
    itemCount = sqlContext.sql("select count(distinct itemId) item_counts from user_behavior").first()
    itemList = sqlContext.sql("select distinct itemId from user_behavior").collect()
    print "totalCount lines in table:" + str(totalCount['row_counts'])
    print "totalCount users in table:" + str(userCount['user_counts'])
    print "totalCount items in table:" + str(itemCount['item_counts'])
    return (totalCount['row_counts'], userCount['user_counts'], itemCount['item_counts'], itemList)

def get_nature_spline(user_active_list):
    print '>>>>>>>>each user action spline'
    spline_list = []
    if user_active_list:
        prev_spline_time = 0
        prev_time = 0
        objectSpline = Spline(user_active_list[0])
        for action in user_active_list:
            prev_spline_time = int(action['timestrap']) - prev_time
            prev_time = action['timestrap']
            if prev_spline_time > SPLINE_LIMIT and prev_time > 0:
                # object save
                # spline_list.append(json.dumps(objectSpline)
                if objectSpline.duration > 0:
                    save_json_file(objectSpline)
                del objectSpline
                objectSpline = Spline(action)
            objectSpline.newAction(action)

def save_json_file(object):
    save_path = Path[5:] + 'user_profiles/uid' + str(object.user_id) + '.json'

    dic = {
        "user_id" : object.user_id,
        "start_timestramp" : object.start_timestramp,
        "duration" : object.start_timestramp,
        "day_of_week" : object.day_of_week,
        "time_of_day" : object.time_of_day,
        "steps" : object.steps,
        "is_buyed" : object.is_buyed,
        "is_faved" : object.is_faved,
        "is_carted" : object.is_carted,
        "spline_pv_count" : object.spline_pv_count,
        "spline_buy_count" : object.spline_buy_count,
        "spline_fav_count" : object.spline_fav_count,
        "spline_cart_count" : object.spline_cart_count,
        "spline_list" : object.spline_list
    }

    with open(save_path,'a') as f:
        json.dump(dic,f)
        print(" user :: " + str(object.user_id) + " spline added !" )

if __name__ == '__main__':
    df = prepareData(sc)
    df.registerTempTable('user_behavior')
    df.cache()
    global_counts = getTableCounts()
    userList = get_user_list()
    for userId in userList:
        userActionList = get_user_action_list(userId[0])
        get_nature_spline(userActionList)
