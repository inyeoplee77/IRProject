import pickle
from konlpy.tag import Kkma
from konlpy.tag import Twitter
import jpype
import os

twitter = Twitter()
f = open('pickle.pickle','rb')
data = pickle.load(f)

#wordbag = []
doc_list = []
termdoc = {}

for datum in data:
	doc_list.append(datum['no'])
	
#data = None
#gc.collect()
	
for datum in data:
	doc_id = datum['no']
	lec_no = datum['lec_no'] #
	pos = twitter.pos(datum['eval_content'],stem = True)
	for p in pos:
		tag = p[1]
		if ('Exclamation' or 'Josa' or 'Eomi' or 'Suffix' or 'Punctuation' or 'Foreign' or 'Alpha' or 'Unknown' or 'KoreanParticle' or 'Hashtag' or 'ScreenName') in tag:
			continue
		if p[0] not in termdoc:
			termdoc[p[0]] = dict.fromkeys(doc_list,0)
		termdoc[p[0]][doc_id] += 1
	print doc_id
'''
tmp = termdoc.keys()
for j in range(10):
	print doc_list[j],
print
for i in range(10):
	print tmp[i],
	for j in range(10):
		print termdoc[tmp[i]][doc_list[j]],
	print
'''

with open('word_bag_pickle.pickle','wb') as f:
	pickle.dump(wordbag,f)
