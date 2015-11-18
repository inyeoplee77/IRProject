import pickle
from konlpy.tag import Kkma
from konlpy.tag import Twitter
import jpype
import os

twitter = Twitter()
f = open('pickle.pickle','rb')
data = pickle.load(f)

wordbag = []
for datum in data:
	pos = twitter.pos(datum['eval_content'],stem = True)
	for p in pos:
		tag = p[1]
		if ('Exclamation' or 'Josa' or 'Eomi' or 'Suffix' or 'Punctuation' or 'Foreign' or 'Alpha' or 'Unknown' or 'KoreanParticle' or 'Hashtag' or 'ScreenName') in tag:
			continue
		word = p[0]
		wordbag.append(word)
with open('word_bag_pickle.pickle','wb') as f:
	pickle.dump(wordbag,f)

