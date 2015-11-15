# -*- coding: utf-8 -*-
from konlpy.tag import Kkma
from pyspark import SparkContext
from pyspark.sql import SQLContext
sc = SparkContext()
sqlContext = SQLContext(sc)

# A JSON dataset is pointed to by path.
# The path can be either a single text file or a directory storing text files.
crawl = sc.pickleFile('merged_file')
#crawl = sqlContext.read.json("all_data/*")#.select('eval_content','professor','lec_name','lec_code').map(lambda x : [x.eval_content,x.professor,x.lec_name,x.lec_code])

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

