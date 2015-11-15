# -*- coding:utf-8 -*-
import sys  
reload(sys)  
sys.setdefaultencoding('utf8')

import glob
import json
def chunks(l, n):
	n = max(1, n)
	return [l[i:i + n] for i in range(0, len(l), n)]

read_files = glob.glob("*.json")
data = []
for f in read_files:
	num = f.split('_')[0]
	#print num
	info = []
	info_file = open(num + '_info.json','r')
	for line in info_file:
		info.append(line)
	eva = json.load(open(num+'_eval.json'))
	if len(eva) == 0:
		continue
	for e in eva:
		e[u'eval_id'] = unicode(num)
		e[u'lec_name'] = unicode(info[0].encode('utf-8'))
		e[u'professor'] = unicode(info[1].encode('utf-8'))
		e[u'lec_code'] = unicode(info[2].encode('utf-8'))
		data.append(e)
	#eva.append({'lec_name':info[0],'professor':info[1],'lec_code':info[2]})
	#data.append(eva)


#chunk = chunks(data,len(data)/20)
#i = 0
#for c in chunk:
#	json.dump(c,open('merged_file'+str(i),'w'))
#	i+=1
#json.dump(data,open('merged_file_large.json','w'))
from pyspark import SparkContext
import pickle

sc = SparkContext()
rdd = sc.parallelize(data)
rdd.saveAsPickleFile('merged_file')

'''
with open("merged_file.json", "wb") as outfile:
	outfile.write('[{}]'.format(','.join([open(f, "rb").read() for f in read_files])))
'''
