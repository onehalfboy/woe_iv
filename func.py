#coding=utf-8
import csv
import datetime
import pymysql
from numpy import *
import types
import copy

debug = False
#xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
sessionId = ""
logDir = "docs/logs"
resultDir = "docs/alist"
gtryCountLimit = 10
partNum = 0
gfields = {}
gstatus = {}
gserver = {}
# _sleep or _callback
fileFlag = ""

def getConfig():
	global debug, logDir, resultDir
	configs = {}
	filename = "./v2a.conf"
	fr = open(filename)
	for line in fr.readlines():
		kv = line.strip().split("=")
		length = len(kv)
		key = kv[0]
		value = ""
		if length >= 2:
			value = kv[1]
		configs[key] = value
	fr.close()
	if "debug" in configs.keys():
		if configs["debug"] == "" or configs["debug"] == "0" or configs["debug"].lower() == "false":
			debug = False
		else:
			debug = True
	if "logdir" in configs.keys():
		if configs["logdir"] == "":
			logDir = "docs/logs"
		else:
			logDir = configs["logdir"]
	if "resultdir" in configs.keys():
		if configs["resultdir"] == "":
			resultDir = "docs/alist"
		else:
			resultDir = configs["resultdir"]
	return True

#python version 2.*
def convert(filename, in_enc = "UTF8", out_enc="GBK"):
    try:
        print "convert " + filename
        content = open(filename).read()
        new_content = content.decode(in_enc).encode(out_enc)
        open(filename, 'w').write(new_content)
        print "done"
    except:
        print "error"

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

def saveCsv(filename, data, mod = 'wb', delimiter='\t', lineterminator='\n'):
	csvfile = file(filename, mod)
	writer = csv.writer(csvfile, delimiter = delimiter, lineterminator = lineterminator)
	writer.writerows(data)
	csvfile.close()

def getCurDate():
	return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def getFileContent(filename):
	f = open(filename, "rb")
	data = f.read()
	f.close()
	return data
	
def setFileContent(filename, content):
	f = open(filename, "wb")
	data = f.write(content)
	f.close()
	return data

def dkwUidsMatch(dkwuidsfile, uidsfile, taguidsfile, dbname = "audio"):
	import pymysql
	dbtConfig = {
		"host":"172.16.0.57",
		"port":3306,
		"user":"root",
		"passwd":"rcroot",
		"db":dbname, #first=>audio, video2=>audio2
		"charset":"utf8",
	}
	# dkwLines = getCsvInfo(dkwuidsfile, "\t", 1, [])
	# dkwCount = len(dkwLines)
	# print "[%s][dkwCount:%d]" % (getCurDate(), dkwCount)

	try:
		connt = pymysql.connect(host=dbtConfig["host"], port=dbtConfig["port"], user=dbtConfig["user"], passwd=dbtConfig["passwd"], db=dbtConfig["db"], charset=dbtConfig["charset"])
		curt = connt.cursor()
	except:
		print "[%s][error][mysql link error]" % (getCurDate())
		print "[%s][stop][dkwUidsMatch]" % (getCurDate())
		return False

	# sqls = []
	# unionCount = 10000
	# for line in dkwLines:
	# 	uid = int(line[0])
	# 	colCount = len(line)
	# 	borrowDate = ""
	# 	if colCount > 1:
	# 		borrowDate = line[1]
	# 	lateDays = ""
	# 	if colCount > 2:
	# 		lateDays = str(line[2])
	# 	if lateDays == "":
	# 		lateDays = "-99999"
	# 		tag = "unknown"
	# 	elif int(lateDays) > 30:
	# 		tag = "bad"
	# 	else:
	# 		tag = "good"
	# 	sqls.append("(%d, '%s', '%s', '%s')" % (uid, borrowDate, lateDays, tag))
	# 	if len(sqls) >= unionCount:
	# 		sql = "insert ignore into `dkw_users` values" + ','.join(sqls)
	# 		# print sql
	# 		# exit()
	# 		row = curt.execute(sql)
	# 		connt.commit()
	# 		sqls = []
	# del dkwLines
	# if len(sqls) >= 1:
	# 	sql = "insert ignore into `dkw_users` values" + ','.join(sqls)
	# 	row = curt.execute(sql)
	# 	connt.commit()
	# 	sqls = []
	# sql2 = 'select count(*) total from `dkw_users` limit 1'
	# row = curt.execute(sql2)
	# info = curt.fetchone()
	# print "[%s][%s:%d]" % (getCurDate(), sql2, int(info[0]))

	lines = getCsvInfo(uidsfile, "\t", 1, [])
	count = len(lines)
	print "[%s][%s:%d]" % (getCurDate(), uidsfile, count)

	sqls = []
	unionCount = 10000
	colCountErrorCount = 0
	# num = 0
	for line in lines:
		# num += 1
		# print num
		if len(line) == 10:
			sqls.append("('%s')" % ("','".join(line)))
		else:
			colCountErrorCount += 1
		if len(sqls) >= unionCount:
			sql = "insert ignore into `audio_users` values" + ','.join(sqls)
			# print sql
			# exit()
			row = curt.execute(sql)
			connt.commit()
			sqls = []
	del lines
	if len(sqls) >= 1:
		sql = "insert ignore into `audio_users` values" + ','.join(sqls)
		row = curt.execute(sql)
		connt.commit()
		sqls = []
	sql2 = 'select count(*) total from `audio_users` limit 1'
	row = curt.execute(sql2)
	info = curt.fetchone()
	print "[%s][%s:%d]" % (getCurDate(), sql2, int(info[0]))
	print "[%s][colCountErrorCount:%d]" % (getCurDate(), colCountErrorCount)
	return True

def video2DkwUidsMatch(video2file, sqlSwitch = False, sql2Switch = True):
	# 贷款王用户基本信息入库
	dbtConfig = {
		"host":"172.16.0.57",
		"port":3306,
		"user":"root",
		"passwd":"rcroot",
		"db":"audio2",
		"charset":"utf8",
	}
	# /root2/tmp/audio2/video/overdue/7.txt
	files = video2file.split("/")
	tableName = "video_%s" % (files[-2])
	sid = int(files[-1].strip(".")[0])
	lines = getCsvInfo(video2file, " ", 1, [])
	dkwCount = len(lines)
	print "[%s][dkwCount:%d]" % (getCurDate(), dkwCount)

	try:
		connt = pymysql.connect(host=dbtConfig["host"], port=dbtConfig["port"], user=dbtConfig["user"], passwd=dbtConfig["passwd"], db=dbtConfig["db"], charset=dbtConfig["charset"])
		curt = connt.cursor()
	except:
		print "[%s][error][mysql link error]" % (getCurDate())
		print "[%s][stop][video2DkwUidsMatch]" % (getCurDate())
		return False

	sqls = []
	sqls2 = []
	unionCount = 10000
	colCountErrorCount = 0
	uidslist = []
	uids = []
	# num = 0
	for line in lines:
		# num += 1
		# print num
		if len(line) == 2:
			uids.append(line[1])
			sqls.append("('%s', -99999)" % ("','".join(line)))
			sqls2.append("(%d, %d)" % (int(line[1]), sid))
		else:
			colCountErrorCount += 1
		if len(sqls) >= unionCount:
			if sqlSwitch:
				sql = "insert ignore into `" + tableName + "` values" + ','.join(sqls)
				# print sql
				# exit()
				row = curt.execute(sql)
				connt.commit()
			if sql2Switch:
				sql2 = "insert ignore into `video_server` values" + ','.join(sqls2)
				# print sql
				# exit()
				row = curt.execute(sql2)
				connt.commit()
			uidslist.append(uids)
			uids = []
			sqls = []
			sqls2 = []
	del lines
	if len(sqls) >= 1:
		if sqlSwitch:
			sql = "insert ignore into `" + tableName + "` values" + ','.join(sqls)
			row = curt.execute(sql)
			connt.commit()
		if sql2Switch:
			sql2 = "insert ignore into `video_server` values" + ','.join(sqls2)
			# print sql
			# exit()
			row = curt.execute(sql2)
			connt.commit()
		uidslist.append(uids)
		uids = []
		sqls = []
		sqls2 = []
	sql2 = "select count(*) total from `" + tableName + "` limit 1"
	row = curt.execute(sql2)
	info = curt.fetchone()
	print "[%s][%s:%d]" % (getCurDate(), sql2, int(info[0]))
	print "[%s][colCountErrorCount:%d]" % (getCurDate(), colCountErrorCount)
	# 匹配贷款王用户
	dkwUidslistMatch(tableName, uidslist, curt, connt)
	curt.close()
	connt.close()
	return True

def dkwUidslistMatch(tableName, uidslist, lcurt, lconnt):
	# print uidslist
	# print tableName, lcurt, lconnt
	# exit()
	dbtConfig = {
		"host":"180.101.195.217",
		"port":5013,
		"user":"fk_fenxi",
		"passwd":"ZmtfZmVueGlAMjM0NS5jb20K",
		"db":"common",
		"charset":"utf8",
	}

	try:
		connt = pymysql.connect(host=dbtConfig["host"], port=dbtConfig["port"], user=dbtConfig["user"], passwd=dbtConfig["passwd"], db=dbtConfig["db"], charset=dbtConfig["charset"])
		curt = connt.cursor()
	except:
		print "[%s][error][mysql link error]" % (getCurDate())
		print "[%s][stop][dkwUidslistMatch]" % (getCurDate())
		exit()
	succCount = 0
	for uids in uidslist:
		uidsStr = ','.join(uids);
		sql2 = 'select user_id, late_days from `all_fin_rank` where user_id in (%s) and rank = 1' % (uidsStr)
		row = curt.execute(sql2)
		rows = curt.fetchall()
		# print rows
		# break
		sqls = []
		for row_1 in rows:
			# print row_1
			if row_1 != None and len(row_1) == 2 and row_1[0] > 0 and row_1[1] != None and row_1[1] != "":
				succCount += 1
				uid = int(row_1[0])
				lateDays = int(row_1[1])
				sqls.append("update `%s` set late_days = %d where uid = %d" % (tableName, lateDays, uid))
		if len(sqls) > 0:
			sql = ';'.join(sqls)
			row = lcurt.execute(sql)
			lconnt.commit()

	curt.close()
	connt.close()
	print "[%s][succCount:%d]" % (getCurDate(), succCount)
	return True

def audioDeal(step, initSql):
	global resultDir
	print "[%s][audioDeal][start]" % (getCurDate())
	dbtConfig = {
		"host":"172.16.0.57",
		"port":3306,
		"user":"root",
		"passwd":"rcroot",
		"db":"audio3",
		"charset":"utf8",
	}
	try:
		connt = pymysql.connect(host=dbtConfig["host"], port=dbtConfig["port"], user=dbtConfig["user"], passwd=dbtConfig["passwd"], db=dbtConfig["db"], charset=dbtConfig["charset"])
		curt = connt.cursor()
	except:
		print "[%s][error][mysql link error]" % (getCurDate())
		print "[%s][stop][audioDeal]" % (getCurDate())
		return False

	if initSql != "":
		row = curt.execute(initSql)
		connt.commit()

	sql = "call audioDeal(%d);" % (step)
	print "[%s][start][%s]" % (getCurDate(), sql)
	row = curt.execute(sql)
	connt.commit()
	print "[%s][stop][%s]" % (getCurDate(), sql)

	fields = {}
	fieldList = []
	sql2 = "select COLUMN_NAME,COLUMN_COMMENT from information_schema.COLUMNs where TABLE_SCHEMA = 'audio3' and TABLE_NAME = 'result_sets'";
	row = curt.execute(sql2)
	rows = curt.fetchall()
	for row in rows:
		field = row[0]
		value = row[1]
		fields[field] = value
		fieldList.append(field)
	fieldCount = len(fieldList)

	datas = []
	data = []
	for i in range(fieldCount):
		field = fieldList[i]
		data.append(fields[field])
	datas.append(data)
	sql3 = "select `" + "`,`".join(fieldList) + "` from `result_sets`"
	row = curt.execute(sql3)
	rows = curt.fetchall()
	curt.close()
	connt.close()
	for row in rows:
		datas.append(row)

	getConfig();
	filename = resultDir + "/result_sets.csv";
	saveCsv(filename, datas, 'wb', ',')
	convert(filename)
	print "[%s][result_sets][count:%d]" % (getCurDate(), len(rows))
	print "[%s][audioDeal][stop]" % (getCurDate())
	return True

def calWoeIv(data, partCount, startCol = 0, stopCol = 0, filename = "woe_iv.csv"):
	header = data[0]
	del data[0]
	needPartCount = partCount
	dataMat = matrix(data)
	if stopCol < 1:
		stopCol = len(data[0]) - 1;
	parts = []
	print "[%s][calWoeIv][startCol:%d][stopCol:%d]" % (getCurDate(), startCol, stopCol)
	#init
	for col in range(startCol, stopCol):
		print "[%s][col:%d][field:%s]" % (getCurDate(), col, header[col])
		colMat = dataMat[:, col]
		colArr = array(colMat)
		colArr2 = []
		for colOne in colArr:
			colArr2.append(float(colOne[0]))
		colArr = colArr2
		minVal = min(colArr)
		maxVal = max(colArr)
		addVal = ceil((maxVal - minVal + 1) / partCount)
		if addVal <= 0:
			print "[error]column index", col, "addVal=%f" % (addVal)
			continue
		partOne = {
			"base": {
				"col": col, 
				"addVal": addVal,
				"minVal": minVal,
				"maxVal": maxVal,
			},
			"parts": [],
			"partsCount": [],
		}
		startVal = minVal
		for i in range(partCount):
			endVal = startVal + addVal - 1
			partOne["parts"].append([startVal, endVal])
			partOne["partsCount"].append({"total":0, "goodCount":0,"badCount":0})
			startVal = endVal + 1
		index = 0
		for row in colArr:
			val = float(row)
			part = int(ceil((val - minVal + 1) / addVal)) - 1
			part = max(part, 0)
			partOne["partsCount"][part]["total"] += 1
			if data[index][-1] == "good":
				partOne["partsCount"][part]["goodCount"] += 1
			else:
				partOne["partsCount"][part]["badCount"] += 1
			index += 1
		parts.append(partOne)
	del data
	print "[%s][init over]" % (getCurDate())
	# 非法数据过滤
	colCount = len(parts)
	partsLast = []
	rules = []
	for colNum in range(colCount):
		partOne = parts[colNum]
		base = partOne["base"]
		base["badCount"] = 0
		base["goodCount"] = 0
		base["total"] = 0
		partOneLast = {
			"base": base,
			"parts": [],
			"partsCount": [],
		}
		partOneRules = {
			"base": copy.copy(base),
			"parts": [],
			"partsCount": [],
		}
		partCount = len(partOne["parts"])
		for part in range(partCount):
			if partOne["partsCount"][part]["goodCount"] < 1 or partOne["partsCount"][part]["badCount"] < 1:
				partOneRules["base"]["badCount"] += partOne["partsCount"][part]["badCount"]
				partOneRules["base"]["goodCount"] += partOne["partsCount"][part]["goodCount"]
				partOneRules["base"]["total"] += partOne["partsCount"][part]["total"]
				partOneRules["parts"].append(partOne["parts"][part])
				partOneRules["partsCount"].append(partOne["partsCount"][part])
			else:
				partOneLast["base"]["badCount"] += partOne["partsCount"][part]["badCount"]
				partOneLast["base"]["goodCount"] += partOne["partsCount"][part]["goodCount"]
				partOneLast["base"]["total"] += partOne["partsCount"][part]["total"]
				partOneLast["parts"].append(partOne["parts"][part])
				partOneLast["partsCount"].append(partOne["partsCount"][part])
		if len(partOneLast["parts"]) > 0:
			partsLast.append(partOneLast)
		if len(partOneRules["parts"]) > 0:
			rules.append(partOneRules)
	del parts
	print "[%s][part data deal over]" % (getCurDate())
	# 分区数量占比 woe 和 iv 计算
	colCount = len(partsLast)
	for colNum in range(colCount):
		partOne = partsLast[colNum]
		baseBadCount = partsLast[colNum]["base"]["badCount"]
		baseGoodCount = partsLast[colNum]["base"]["goodCount"]
		baseTotal = partsLast[colNum]["base"]["total"]
		partsLast[colNum]["base"]["badPercent"] = round(baseBadCount * 100.0 / baseTotal, 2)
		baseBadWoe = 0.0
		baseBadIv = 0.0
		partCount = len(partOne["parts"])
		for part in range(partCount):
			partOne["partsCount"][part]["badPercent"] = round(partOne["partsCount"][part]["badCount"] * 100.0 / partOne["partsCount"][part]["total"], 2)
			badBasePer = partOne["partsCount"][part]["badCount"] * 1.0 / baseBadCount
			goodBasePer = partOne["partsCount"][part]["goodCount"] * 1.0 / baseGoodCount
			badWoe = math.log(badBasePer / goodBasePer, math.e)
			partOne["partsCount"][part]["badWoe"] = badWoe
			baseBadWoe += badWoe
			badIv = (badBasePer - goodBasePer) * badWoe
			partOne["partsCount"][part]["badIv"] = badIv
			baseBadIv += badIv
			partOne["partsCount"][part]["partCountPercent"] = round(partOne["partsCount"][part]["total"] * 100.0 / baseTotal, 2)
		partsLast[colNum]["base"]["badWoe"] = baseBadWoe
		partsLast[colNum]["base"]["badIv"] = baseBadIv
	print "[%s][woe iv over]" % (getCurDate())
	#获取结果报表
	datas = []
	featMergeHeader = ["[列]特征", "实际/期望分区数", "最小值", "最大值", "bad数量", "good数量", "合计", "bad比例(%)", "bad WOE", "bad IV"]
	featMergeFooter = ["#", "#", "#", "#", "#", "#", "#", "#", "#", "#"]
	featMerge = []
	colCount = len(partsLast)
	for colNum in range(colCount):
		partOne = partsLast[colNum]
		feat = "[%d]%s" % (partOne["base"]["col"], header[partOne["base"]["col"]])
		row = [feat, "bad数量", "good数量", "合计", "分区数量占比(%)", "bad比例(%)", "bad WOE", "bad IV"]
		datas.append(row)
		partCount = len(partOne["parts"])
		for part in range(partCount):
			col1 = partOne["parts"][part]
			col2 = partOne["partsCount"][part]["badCount"]
			col3 = partOne["partsCount"][part]["goodCount"]
			col4 = partOne["partsCount"][part]["total"]
			col5 = partOne["partsCount"][part]["partCountPercent"]
			col6 = partOne["partsCount"][part]["badPercent"]
			col7 = partOne["partsCount"][part]["badWoe"]
			col8 = partOne["partsCount"][part]["badIv"]
			row = [col1, col2, col3, col4, col5, col6, col7, col8]
			datas.append(row)
		col1 = "合计"
		col2 = partOne["base"]["badCount"]
		col3 = partOne["base"]["goodCount"]
		col4 = partOne["base"]["total"]
		col5 = 100.0
		col6 = partOne["base"]["badPercent"]
		col7 = partOne["base"]["badWoe"]
		col8 = partOne["base"]["badIv"]
		row = [col1, col2, col3, col4, col5, col6, col7, col8]
		datas.append(row)
		featMerge.append([feat, "%d/%d" % (partCount, needPartCount), partOne["base"]["minVal"], partOne["base"]["maxVal"], col2, col3, col4, col6, col7, col8])
		row = ["#", "#", "#", "#", "#", "#", "#", "#"]
		datas.append(row)
	colCount = len(rules)
	if colCount > 0:
		row = ["规则如下", "#", "#", "#", "#", "#", "#"]
		datas.append(row)
		# 规则
		for colNum in range(colCount):
			partOne = rules[colNum]
			col1 = "[%d]%s" % (partOne["base"]["col"], header[partOne["base"]["col"]])
			row = [col1, "bad数量", "good数量", "合计", "bad比例(%)"]
			datas.append(row)
			partCount = len(partOne["parts"])
			for part in range(partCount):
				col1 = partOne["parts"][part]
				col2 = partOne["partsCount"][part]["badCount"]
				col3 = partOne["partsCount"][part]["goodCount"]
				col4 = partOne["partsCount"][part]["total"]
				if col4 <= 0 or col2 <= 0:
					col5 = 0.0
				else:
					col5 = round(col2 * 100.0 / col4, 2)
				row = [col1, col2, col3, col4, col5]
				datas.append(row)
			col1 = "合计"
			col2 = partOne["base"]["badCount"]
			col3 = partOne["base"]["goodCount"]
			col4 = partOne["base"]["total"]
			if col4 <= 0 or col2 <= 0:
				col5 = 0.0
			else:
				col5 = round(col2 * 100.0 / col4, 2)
			row = [col1, col2, col3, col4, col5]
			datas.append(row)
			row = ["#", "#", "#", "#", "#"]
			datas.append(row)
	datas.insert(0, featMergeFooter)
	featMerge = sorted(featMerge, key = lambda featMerge: featMerge[9])
	for row in featMerge:
		datas.insert(0, row)
	datas.insert(0, featMergeHeader)
	print "[%s][result over]" % (getCurDate())
	# print datas
	saveCsv(filename, datas, mod = 'wb', delimiter=',')
	print "[%s][save over]" % (getCurDate())
	convert(filename)
	print "[%s][convert over]" % (getCurDate())
	return True
