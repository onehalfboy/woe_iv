#coding=utf-8
#python featBoxImg.py "" 1 0 "csv/bigFields.tsv"

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

def getFeatImg(rowHead, allInfo, fileFlag, show = False):
    labels = ['good', 'bad']
    good = []
    bad = []
    selectCol = int(fileFlag)
    selectColName = rowHead[0][selectCol]
    dataList = []
    datingLabels = []
    for one in allInfo:
        # #特殊处理，是语音文本，得到其长度
        # if selectCol == 1:
        #     tag = one[-1]
        #     tagType = type(tag)
        #     if tagType == types.String:
        #         if tag == 'good':
        #             # print selectCol, one[selectCol]
        #             good.append(len(one[selectCol]))
        #         elif tag == 'bad':
        #             bad.append(len(one[selectCol]))
        #     elif tagType == types.Integer:
        #         if tag == 1:
        #             # print selectCol, one[selectCol]
        #             good.append(len(one[selectCol]))
        #         elif tag == 0:
        #             bad.append(len(one[selectCol]))
        #     continue
        if one[selectCol] == '':
            continue;
        tag = one[-1]
        if not tag.isnumeric():
            if tag == 'good':
                # print selectCol, one[selectCol]
                good.append(int(one[selectCol]))
            elif tag == 'bad':
                bad.append(int(one[selectCol]))
        else:
            tag = int(tag)
            if tag == 1:
                # print selectCol, one[selectCol]
                good.append(int(one[selectCol]))
            elif tag == 0:
                bad.append(int(one[selectCol]))

    #show picture
    # from numpy import *
    import matplotlib.pyplot as plt
    from matplotlib.font_manager import FontProperties
    font = FontProperties(fname='/usr/share/fonts/truetype/droid/DroidSansFallbackFull.ttf', size=14)
    fig = plt.figure()
    ax = fig.add_subplot(111)
    dataMat = [mat([good]),mat([bad])]
    # plt.boxplot(dataMat, labels = labels, whis = 1.5)
    plt.boxplot(dataMat, labels = labels)

    title = 'audio_column_' + str(fileFlag) + '_' + selectColName
    ax.set_title(title, fontproperties=font)
    plt.xlabel('好坏', fontproperties=font)
    plt.ylabel(selectColName, fontproperties=font)
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
