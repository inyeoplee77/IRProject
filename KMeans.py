# -*- coding: utf-8 -*-
from konlpy.tag import Twitter
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.mllib.feature import HashingTF
from pyspark.mllib.feature import IDF
from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from pyspark.ml.feature import Normalizer
from pyspark.mllib.clustering import KMeans, KMeansModel
import pickle
from numpy import array


sc = SparkContext()
sqlContext = SQLContext(sc)

normData = sc.pickleFile('idf_normalized')

from pyspark.mllib.clustering import KMeans, KMeansModel
from math import sqrt
data = normData.map(lambda x : x.idf_norm)
clusters = KMeans.train(data, 10, maxIterations=10,runs=10, initializationMode="random")
'''
def error(point):
	center = clusters.centers[clusters.predict(point)]
	return sqrt(sum([x**2 for x in (point - center)]))
'''
clusters.save(sc,'KMeansModel')
#WSSSE = data.map(lambda point: error(point)).reduce(lambda x, y: x + y)
#print("Within Set Sum of Squared Error = " + str(WSSSE))


