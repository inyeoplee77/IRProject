# -*- coding: utf-8 -*-
from konlpy.tag import Twitter
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.mllib.feature import HashingTF
from pyspark.mllib.feature import IDF
from pyspark.ml.feature import HashingTF, IDF, Tokenizer

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

documents = sqlContext.createDataFrame(sc.pickleFile('merged_file').map(lambda x : (x['no'], create_wordbag(x))),['no','words'])
htf = HashingTF(inputCol= 'words',outputCol = 'rawFeatures')
featured = htf.transform(documents)
idf = IDF(inputCol = 'rawFeatures',outputCol = 'idf')
idfModel = idf.fit(featured)
tf_idf = idfModel.transform(featured)
for data in tf_idf.select('no','idf').take(10):
	print data
