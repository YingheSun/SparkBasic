from pyspark import SparkContext
import random
sc = SparkContext("local")

def random_int_list(start, stop, length):
  start, stop = (int(start), int(stop)) if start <= stop else (int(stop), int(start))
  length = int(abs(length)) if length else 0
  random_list = []
  for i in range(length):
    random_list.append(random.randint(start, stop))
  return random_list


intRDD=sc.parallelize(random_int_list(1,100,10000),10)
print intRDD.collect()
print intRDD.count()

def makeDouble(x):
    return 2*x

newRDD1 = intRDD.map(makeDouble).collect()
print newRDD1

newRDD2 = intRDD.map(lambda x:x+100).collect()
print newRDD2

filter1 = intRDD.filter(lambda x:x<50).count()
print filter1

filter2 = intRDD.filter(lambda x: 19 < x and x < 70 or x > 90 ).collect()
print filter2

sRDD = intRDD.randomSplit([0.2,0.6,0.2])
print sRDD[0].collect()
print sRDD[0].count()
print sRDD[1].collect()
print sRDD[1].count()

groupRDD = intRDD.groupBy(
    lambda x:"group1" if (x%3 == 1) else "group2"
).collect()

print (groupRDD[0][0],sorted(groupRDD[0][1]))
print (groupRDD[1][0],sorted(groupRDD[1][1]))

unionRDD = sRDD[0].union(sRDD[2])
print unionRDD.count()

print unionRDD.first()
print unionRDD.take(13)
print unionRDD.takeOrdered(1500)
print unionRDD.takeOrdered(3000,key = lambda x:-x)