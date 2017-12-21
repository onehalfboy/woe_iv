#coding=utf-8
#python featDotImg.py "" 1 0 "csv/bigFields.tsv"

import sys
reload(sys)
sys.setdefaultencoding('utf8')

csvfile = 'csv/bigFields.tsv'
fileFlag = ""
#特征列开始索引，从0开始
startCol=0
#特征列结束索引，从0开始，0表示倒数第2列，最后一列是标签（good bad），该索引的特征将不计算
endCol=0
if len(sys.argv) > 1:
    fileFlag = sys.argv[1]
if len(sys.argv) > 2:
    startCol = int(sys.argv[2])
if len(sys.argv) > 3:
    endCol = int(sys.argv[3])
if len(sys.argv) > 4:
    csvfile = sys.argv[4]

from numpy import *

import func
import types
allInfo = func.getCsvInfo(csvfile, '\t', 1, [])
rowHead = allInfo[0:1]
# print rowHead
# exit()
del allInfo[0:1]

tag = allInfo[0][-1]
if not tag.isnumeric():
	count = len(allInfo)
	for i in range(count):
		tag = allInfo[i][-1]
		flag = -1
		if tag == 'good':
			flag = 1
		elif tag == 'bad':
			flag = 0
		allInfo[i][-1] = flag

def getFeatImg(rowHead, allInfo, fileFlag, show = False):
	selectCol = int(fileFlag)
	selectColName = rowHead[0][selectCol]
	dataList = []
	datingLabels = []
	# count = 0
	for one in allInfo:
		flag = int(one[-1])
		# #特殊处理，是语音文本，得到其长度
		# if selectCol == 1:
		# 	data = [flag, float(len(one[selectCol]))]
		# 	dataList.append(data)
		# 	datingLabels.append(flag)
		# 	continue
		if one[selectCol] == '':
			continue;
		data = [flag, float(one[selectCol])]
		dataList.append(data)
		datingLabels.append(flag)
	datingDataMat = mat(dataList)

	#show picture
	# from numpy import *
	import matplotlib
	import matplotlib.pyplot as plt
	from matplotlib.font_manager import FontProperties
	font = FontProperties(fname='/usr/share/fonts/truetype/droid/DroidSansFallbackFull.ttf', size=14)
	fig = plt.figure()
	ax = fig.add_subplot(111)
	title = 'dot_audio_column_' + str(fileFlag) + '_' + selectColName
	ax.set_title(title, fontproperties=font)
	plt.xlabel('好坏（1，0）', fontproperties=font)
	plt.ylabel(selectColName, fontproperties=font)
	# ax.scatter(datingDataMat[:,0], datingDataMat[:,1], 15.0*(array(datingLabels) + 2), 15.0*(array(datingLabels) + 2))
	ax.scatter(array(datingDataMat[:,0]), array(datingDataMat[:,1]), 15.0*(array(datingLabels) + 2), 15.0*(array(datingLabels) + 2))
	plt.savefig("img/" + title + ".jpg")
	if show:
		plt.show()

if fileFlag != '':
	getFeatImg(rowHead, allInfo, fileFlag, True)
else:
	if endCol < 1:
		endCol = len(allInfo[0]) - 1
	featColCount = endCol - startCol
	# print featColCount, startCol + featColCount - 1
	for i in range(featColCount):
		getFeatImg(rowHead, allInfo, startCol + i)
	print 'feat column number ', startCol, '-', startCol + featColCount - 1, ' ok'
