import pickle
from konlpy.tag import Kkma
from konlpy.tag import Twitter
import jpype
import os

twitter = Twitter()
f = open('pickle','rb')
data = pickle.load(f)

wordbag = []
for datum in data:
	pos = twitter.pos(datum['eval_content'])
	for p in pos:
		tag = p[1]
		if ('JK' or 'JX' or 'JC' or 'EP' or 'EF' or 'EC' or 'ET' or 'XP' or 'XS' or 'SF' or 'SP' or 'SS' or 'SE' or 'SO' or 'SW' or 'OH' or 'OL') in tag:
			continue
		word = p[0]
		wordbag.append(word)
with open('word_bag_pickle','wb') as f:
	pickle.dump(wordbag,f)

