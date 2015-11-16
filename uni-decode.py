# -*- coding: utf-8 -*-
from konlpy.tag import Kkma
from pyspark import SparkContext
from pyspark.sql import SQLContext
sc = SparkContext()
#kkma = Kkma()
#wordbag = []

def create_wordbag(x):
	wordbag = []
	kkma = Kkma()
	if(x['eval_content']) is None:
		return wordbag
	for text in kkma.pos(x['eval_content']):
		tag = text[1]
		if ('JK' or 'JX' or 'JC' or 'EP' or 'EF' or 'EC' or 'ET' or 'XP' or 'XS' or 'SF' or 'SP' or 'SS' or 'SE' or 'SO' or 'SW' or 'OH' or 'OL') in tag:
			continue
		word = text[0]
		if word in wordbag:
			return wordbag
		wordbag.append(word)
	return wordbag

words = sc.pickleFile('IRProject/merged_file').flatMap(lambda x : create_wordbag(x)).distinct()
print type(words)
for data in words.collect():
	print data
	
		
		
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
