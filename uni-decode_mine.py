# -*- coding: utf-8 -*-
from konlpy.tag import Twitter
from pyspark import SparkContext
from pyspark.sql import SQLContext

sc = SparkContext()

unneeded = [u'Unkown', u'KoreanParticle', u'Hashtag', u'ScreenName', u'Number', u'Alpha', u'Foreign', u'Punctuation', u'Suffix', u'Eomi', u'PreEomi', u'Josa', u'Exclamation']

def create_dictionary(x):
	wordbag = []
	if (x['eval_content']) is None:
		return wordbag

	twitter = Twitter()
	for text in twitter.pos(x['eval_content'], stem = True):
		tag = text[1]
		if tag in unneeded:
			continue

		word = text[0]
		if word in wordbag:
			continue 
	
		wordbag.append((word))

	return wordbag

'''
def create_matrix(x, terms, matrix):
	
	if (x['eval_content']) is None:
		return matrix
	for text in twitter.pos(x['eval_content'], stem = True):
		matrix.index((text)
'''

terms = sc.pickleFile('merged_file').flatMap(lambda x : create_dictionary(x)).distinct()
#print terms.count()

matrix_key = terms.collect()
if len(matrix_key) % 2:
	matrix_key.append("")

matrix = dict((k, []) for k in matrix_key)



'''
f = open('dictionary_test.txt', 'w')
for m in matrix:
	f.write(m)
	f.write('	')
'''

#read = sc.pickleFile('merged_file').map(lambda x: create_matrix(x, terms, matrix)

#terms.saveAsPickleFile('bag_of_words2')

#terms = sc.pickleFile('bag_of_words')
#for term in terms.take(100):
#	print term.encode('utf-8')
		
'''
f = open('json_parsed.txt','w')
kkma = Kkma()
for data in crawl.collect():
	evaluation = data['eval_content']
	#print data
	if evaluation is None:
		continue
	#evaluation = evaluation text
	f.write(data['eval_id'])
	f.write(' ')
	f.write(data['professor'].encode('utf-8'))
	f.write(' ')
	f.write(data['lec_name'].encode('utf-8'))
	f.write(' ')
	f.write(data['lec_code'].encode('utf-8'))
	f.write(' ')
	for pos in kkma.pos(evaluation):
		f.write(pos[0].encode('utf-8'))
		f.write('/')
		f.write(pos[1].encode('utf-8'))
		f.write(' ')
	f.write('\n')
'''
