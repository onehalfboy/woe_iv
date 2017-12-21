#coding=utf-8

#数据保存到csv文件
def saveCsv(filename, data):
	import codecs
	import csv
	csvfile = file(filename, 'wb')
	csvfile.write(codecs.BOM_UTF8)
	writer = csv.writer(csvfile)
	writer.writerows(data)
	csvfile.close()

#获取csv文件内容，可以指定分隔符号，数据起始行（从1开始），指定要取的数据列号例如：[['A', '字段名', '描述'], ['BX', 'id', '行号']]
def getCsvInfo(filename, field, dataStartRow, featlist):
	returnMat = []
	fr = open(filename)
	row = 0.0
	featCount = len(featlist)
	featcols = []
	if featCount > 0:
		for cols in featlist:
			ab = cols[0].upper()
			charCount = len(ab)
			if charCount == 1:
				col = int(ord(ab) - ord('A'))
				featcols.append(col)
			elif charCount == 2:
				col = (int(ord(ab[0]) - ord('A')) + 1) * 26 + int(ord(ab[1]) - ord('A'))
				featcols.append(col)
	# print featcols
	# exit()				
	for line in fr.readlines():
		line = line.decode('utf-8')
		row += 1.0
		if row < dataStartRow:
			continue
		lines = line.strip().split(field)
		one = []
		if featCount < 1:
			one = lines
		else:
			for col in featcols:
				# print row, col, lines
				one.append(lines[col])
		# print one, lines
		# exit()
		returnMat.append(one)
	fr.close()
	return returnMat

#获取用户信息
def getUserInfo(filename):
	userInfo = getCsvInfo(filename, '\t', 2, [])
	# userInfo = getCsvInfo(filename, '\t', 2, [['B', 1], ['d', 2]])
	# print userInfo[0]
	return userInfo

#合并数据到一个csv文件
def setUnionCsv(saveCsvfile):
	userCsvfile = 'csv/base/br_user_id.csv'
	userInfo = getUserInfo(userCsvfile)
	# print userInfo[0]
	scoreFeatlistCsvfile = 'csv/base/br_score_featlist.csv'
	scoreFeatlist = getCsvInfo(scoreFeatlistCsvfile, '\t', 1, [])
	# print scoreFeatlist
	scoreCsvfile = 'csv/base/br_score.csv'
	scoreInfo = getCsvInfo(scoreCsvfile, ',', 3, scoreFeatlist)
	specialFeatlistCsvfile = 'csv/base/br_special_featlist.csv'
	specialFeatlist = getCsvInfo(specialFeatlistCsvfile, '\t', 1, [])
	# print specialFeatlist
	specialCsvfile = 'csv/base/br_special.csv'
	specialInfo = getCsvInfo(specialCsvfile, ',', 3, specialFeatlist)
	# print specialInfo[0]
	userCount = len(userInfo)
	for unum in range(userCount):
		user = userInfo[unum]
		id = user[1]
		count = len(scoreInfo)
		for i in range(count):
			if scoreInfo[i][0] == id:
				userInfo[unum].extend(scoreInfo[i][1:])
				del(scoreInfo[i])
				break
		count = len(specialInfo)
		for i in range(count):
			if specialInfo[i][0] == id:
				userInfo[unum].extend(specialInfo[i][1:])
				del(specialInfo[i])
				break
		# print userInfo[0]
		# exit()
	rowHead = [['1','2','3','4','5'], ['id','br_id','user_id','type','late_days'], ['行号','身份证号','用户ID','预期类型','逾期天数']]
	del(scoreFeatlist[0])
	count = len(scoreFeatlist)
	snum = len(rowHead[0]);
	for i in range(count):
		rowHead[0].append(str(snum + i + 1))
		rowHead[1].append(scoreFeatlist[i][1])
		rowHead[2].append(scoreFeatlist[i][2])
	del(specialFeatlist[0])
	count = len(specialFeatlist)
	snum = len(rowHead[0]);
	for i in range(count):
		rowHead[0].append(str(snum + i + 1))
		rowHead[1].append(specialFeatlist[i][1])
		rowHead[2].append(specialFeatlist[i][2])
	# print rowHead
	data = rowHead + userInfo
	saveCsv(saveCsvfile, data)

def unionCsv(union):
	saveCsvfile = 'csv/base/br_feat_all.csv'
	if union:
		#合并数据
		setUnionCsv(saveCsvfile)
	else:
		#划分标签
		allInfo = getCsvInfo(saveCsvfile, ',', 1, [])
		rowHead = allInfo[0:3]
		maxCol = len(rowHead[0])
		rowHead[0].append(str(maxCol + 1))
		rowHead[1].append('tag')
		rowHead[2].append('标签')
		del(allInfo[:3])
		count = len(allInfo)
		# good = ['MN', 'M0']
		good = ['MN', 'M0', 'M1', 'M2']
		for i in range(count):
			tag = 'bad'
			type = allInfo[i][3]
			if type in good:
				tag = 'good'
			allInfo[i].append(tag)
		data = rowHead + allInfo
		saveTagCsvfile = 'csv/base/br_feat_all_tag.csv'
		saveCsv(saveTagCsvfile, data);

def clearUnionCsv():
	saveCsvfile = 'csv/base/br_feat_all_tag.csv'
	#清理数据
	allInfo = getCsvInfo(saveCsvfile, ',', 1, [])
	rowHead = allInfo[0:3]
	colCount = len(rowHead[0])
	del(allInfo[:3])
	count = len(allInfo)
	# good = ['MN', 'M0']
	good = ['MN', 'M0', 'M1', 'M2']
	lastData = []
	for i in range(count):
		delFlag = False
		for j in range(colCount):
			if allInfo[i][j] == '':
				delFlag = True
				break
		if delFlag == False:
			lastData.append(allInfo[i])
	data = rowHead + lastData
	saveClearCsvfile = 'csv/base/br_feat_all_tag_clear.csv'
	saveCsv(saveClearCsvfile, data);

def unionCsv2(union):
	saveCsvfile = 'csv/base/br_feat_all_2.csv'
	if union:
		#合并数据
		setUnionCsv2(saveCsvfile)
	else:
		#划分标签
		allInfo = getCsvInfo(saveCsvfile, ',', 1, [])
		rowHead = allInfo[0:3]
		maxCol = len(rowHead[0])
		rowHead[0].append(str(maxCol + 1))
		rowHead[1].append('tag')
		rowHead[2].append('标签')
		del(allInfo[:3])
		count = len(allInfo)
		# good = ['MN', 'M0']
		good = ['MN', 'M0', 'M1', 'M2']
		for i in range(count):
			tag = 'bad'
			type = allInfo[i][3]
			if type in good:
				tag = 'good'
			allInfo[i].append(tag)
		data = rowHead + allInfo
		saveTagCsvfile = 'csv/base/br_feat_all_tag.csv'
		saveCsv(saveTagCsvfile, data);

#合并数据到一个csv文件
def setUnionCsv2(saveCsvfile):
	userCsvfile = 'csv/base/br_user_id.csv'
	userInfo = getUserInfo(userCsvfile)
	# print userInfo[0]
	scoreFeatlistCsvfile = 'csv/base/br_id2_featlist.csv'
	scoreFeatlist = getCsvInfo(scoreFeatlistCsvfile, '\t', 1, [])
	# print scoreFeatlist
	scoreCsvfile = 'csv/base/br_id2.csv'
	scoreInfo = getCsvInfo(scoreCsvfile, '\t', 3, scoreFeatlist)
	specialFeatlistCsvfile = 'csv/base/br_mobile3_featlist.csv'
	specialFeatlist = getCsvInfo(specialFeatlistCsvfile, '\t', 1, [])
	# print specialFeatlist
	specialCsvfile = 'csv/base/br_mobile3.csv'
	specialInfo = getCsvInfo(specialCsvfile, ',', 3, specialFeatlist)
	# print specialInfo[0]
	userCount = len(userInfo)
	for unum in range(userCount):
		user = userInfo[unum]
		id = user[1]
		count = len(scoreInfo)
		for i in range(count):
			if scoreInfo[i][0] == id:
				userInfo[unum].extend(scoreInfo[i][1:])
				del(scoreInfo[i])
				break
		count = len(specialInfo)
		for i in range(count):
			if specialInfo[i][0] == id:
				userInfo[unum].extend(specialInfo[i][1:])
				del(specialInfo[i])
				break
		# print userInfo[0]
		# exit()
	rowHead = [['1','2','3','4','5'], ['id','br_id','user_id','type','late_days'], ['行号','身份证号','用户ID','预期类型','逾期天数']]
	del(scoreFeatlist[0])
	count = len(scoreFeatlist)
	snum = len(rowHead[0]);
	for i in range(count):
		rowHead[0].append(str(snum + i + 1))
		rowHead[1].append(scoreFeatlist[i][1])
		rowHead[2].append(scoreFeatlist[i][2])
	del(specialFeatlist[0])
	count = len(specialFeatlist)
	snum = len(rowHead[0]);
	for i in range(count):
		rowHead[0].append(str(snum + i + 1))
		rowHead[1].append(specialFeatlist[i][1])
		rowHead[2].append(specialFeatlist[i][2])
	# print rowHead
	data = rowHead + userInfo
	saveCsv(saveCsvfile, data)

# unionCsv(True)
# unionCsv(False)
# clearUnionCsv()
