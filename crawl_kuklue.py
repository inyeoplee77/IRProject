import os
for root, dirs, files in os.walk("./crawl_data"):
	for name in files:
		f = open(os.path.join(root, name))
		s = str(f.name)


		num = s.split('/')
		if len(num) <= 3:
			continue
		num = num[3].split('_')[0]
		
		if 'info' not in s:
			continue
		info = []
		count = 0
		f_write = open(os.path.join(root,num + '_.txt'),"w")
		for line in f:
			count += 1
			if '\"lec_name\"' in line:
				info.append(line.split('<span>')[1].split('</span>')[0])
				print line.split('<span>')[1].split('</span>')[0]
			if 'lec_profName' in line:
				info.append(line.split('<span>')[1].split('</span>')[0])
			if count == 13:
				info.append(line.split('</span>')[1].split(">")[-1])
		for line in info:
			f_write.write(line + '\n')
	

