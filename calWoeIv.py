#coding=utf-8
#python calWoeIv.py 10 1 0 csv/bigFields.tsv csv/woe_iv-bigFields.csv
import sys
reload(sys)
sys.setdefaultencoding('utf8')
import func
#等分分区数
partCount=10
#特征列开始索引，从0开始
startCol=0
#特征列结束索引，从0开始，0表示倒数第2列，最后一列是标签（good bad），该索引的特征将不计算
endCol=0
datafilename="csv/bigFields.tsv"
filename="csv/woe_iv.csv"
if len(sys.argv) > 1:
	partCount = int(sys.argv[1])
if len(sys.argv) > 2:
	startCol = int(sys.argv[2])
if len(sys.argv) > 3:
	endCol = int(sys.argv[3])
if len(sys.argv) > 4:
	datafilename = sys.argv[4]
if len(sys.argv) > 5:
	filename = sys.argv[5]

print "[%s][calWoeIv]file %s loading ..." % (func.getCurDate(), datafilename)

# data = [
# 	['列0','列1','列2','列3','列4','列5','列6','列7','列8','列9','列10','标签'],
# 	[1,2,3,4,5,6,7,8,9,10, 'bad'],
# 	[2,2324,314,414,513414,61431,7141,8141,9141,10141, 'good'],
# 	[3,2411,314,441414,514546,6657,753632,81434,911,101435, 'bad'],
# 	[4,25363,367657,478445,523451,641325,713452,8152,96356,1034564, 'bad'],
# 	[5,24635,343563,4532456,54325,6657,75476,83645,9436,105363, 'good'],
# 	[6,2465,3563,42456,525,657,7546,8345,936,10363, 'bad'],
# 	[7,245,363,4456,55,57,746,845,36,363, 'good'],
# 	[8,2425,3623,44256,525,572,7246,8245,326,3623, 'bad'],
# 	# ['good','bad','good','good','bad','good','bad','good','bad','bad'],
# ]
data = func.getCsvInfo(datafilename, "\t", 1, [])
print "[%s][calWoeIv][data row count:%d]" % (func.getCurDate(), len(data))
func.calWoeIv(data, partCount, startCol, endCol, filename)
