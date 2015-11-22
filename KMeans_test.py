# -*- coding: utf-8 -*-
from konlpy.tag import Twitter
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.mllib.feature import HashingTF
from pyspark.mllib.feature import IDF
from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from pyspark.ml.feature import Normalizer
from pyspark.mllib.clustering import KMeans, KMeansModel
from numpy import array
from math import sqrt


sc = SparkContext()
sqlContext = SQLContext(sc)

normData = sc.pickleFile('idf_normalized')
clusters = KMeansModel.load('KMeasModel')
text = normData.map(lambda x : (x.no,x.eval_content))
data = normData.map(lambda x : (x.no,clusters.predict(x.idf_norm)) )
result = text.join(data).map(lambda (k, (left,right)) : (right,left.encode('uft-8')) )
for i in range(10):
	result.filter(lambda (x,y): x == i).map( lambda (x,y): y).saveAsTextFile("KMeansOutput/cluster_"+str(i))

