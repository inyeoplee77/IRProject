# -*- coding: utf-8 -*-
from konlpy.utils import pprint
from konlpy.tag import Kkma
from konlpy.tag import Twitter

kkma = Kkma()
twitter = Twitter()
pprint(twitter.pos(u'수업의 전달력이 높았습니다. 판서도 아주 잘 해주시고요ㅎㅎ 수업 흐름도 따라가기 쉽게, 중간중간 했던 것도 다시 강의해 주셔서 좋았습니다. 과제부담도 없고요ㅎㅎ'))
pprint(kkma.pos(u'수업의 전달력이 높았습니다. 판서도 아주 잘 해주시고요ㅎㅎ 수업 흐름도 따라가기 쉽게, 중간중간 했던 것도 다시 강의해 주셔서 좋았습니다. 과제부담도 없고요ㅎㅎ'))
