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


sc = SparkContext()
sqlContext = SQLContext(sc)
unneeded = [u'Unknown', u'KoreanParticle',u'Hashtag', u'ScreenName' ,u'Number', u'Alpha', u'Foreign',u'Punctuation', u'Suffix', u'Eomi', u'PreEomi' ,u'Josa', u'Exclamation']

def create_wordbag(x):
	wordbag = []
	if(x['eval_content']) is None:
		return wordbag	
	twitter = Twitter()
	for text in twitter.pos(x['eval_content'], stem = True):
		tag = text[1]
		if tag in unneeded:
			continue

		word = text[0]
		wordbag.append(word)
	return wordbag
	

documents = sqlContext.createDataFrame(sc.pickleFile('merged_file/part-00000').map(lambda x : [x['eval_id'],x['no'],create_wordbag(x),x['professor'],x['lec_code'][:4],x['lec_code'][5],x['eval_total'],x['eval_id']]),['eval_id','no','words','prof_name','department','grade','eval_total','eval_id'])

#users = sqlContext.createDataFrame(sc.pickleFile('merged_file').map(lambda x : (x['mb_no'],x['lec_code'][:4])),['user','department']).orderBy('department')
#for u in users.select('department','user').take(10000):
#	print u
'''
professors = documents.select('prof_name').distinct()
department = documents.select('department').distinct()
#grade	1/2/3/4
eval_total = documents.select('eval_total').distinct() # 1/2/3/4/5

for e in eval_total.collect():
	print e
'''



htf = HashingTF(inputCol= 'words',outputCol = 'rawFeatures')
featured = htf.transform(documents)
idf = IDF(inputCol = 'rawFeatures',outputCol = 'idf')
idfModel = idf.fit(featured)
tf_idf = idfModel.transform(featured)
normalizer = Normalizer(inputCol = 'idf', outputCol = 'idf_norm', p = 2.0)
normData = normalizer.transform(tf_idf)

normData.rdd.saveAsPickleFile('idf_normalized')

