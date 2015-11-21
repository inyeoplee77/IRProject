# -*- coding: utf-8 -*-
#from konlpy.tag import Kkma
from konlpy.tag import Twitter
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.mllib.feature import HashingTF
from pyspark.mllib.feature import IDF
sc = SparkContext()

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
documents = sc.pickleFile('merged_file').map(lambda x : (x['no'],create_wordbag(x)))
htf = HashingTF()
tf_id = documents.mapValues(htf.transform)
tf_id.cache()
#for a in tf_id.take(100):
#	print a
#tf = htf.transform(documents.values())
#tf.cache()
idf = IDF().fit(tf_id.values())
tfidf_id = tf_id.mapValues(idf.transform)

print type(tfidf_id)


for a in tfidf_id.take(10):
	print a

#for vector in tfidf.take(100):
#	print vector
#words.saveAsPickleFile('bag_of_words')
#words = sc.pickleFile('bag_of_words')
#for word in words.take(100):
#	print word.encode('utf-8')
