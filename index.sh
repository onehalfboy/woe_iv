#!/bin/bash
#bash index.sh 10 1 0 csv/bigFields.tsv csv/woe_iv-bigFields.csv
#等分分区数
partCount=10
#特征列开始索引，从0开始
startCol=0
#特征列结束索引，从0开始，0表示倒数第2列，最后一列是标签（good bad），该索引的特征将不计算
endCol=0
#待分析的数据文件
datafilename="csv/bigFields.tsv"
#最后的结果文件
resultfilename="csv/woe_iv-bigFields.tsv"

if [ ! -z "$1" ]; then
	partCount="$1"
fi
if [ ! -z "$2" ]; then
	startCol="$2"
fi
if [ ! -z "$3" ]; then
	endCol="$3"
fi
if [ ! -z "$4" ]; then
	datafilename="$4"
fi
if [ ! -z "$5" ]; then
	resultfilename="$5"
fi
if [ ! -d "img/" ]; then
	mkdir -p "img/"
fi
echo "[start]"`date '+%Y-%m-%d %H:%M:%S'`

# source ~/.bashrc

#woe iv
echo "[calWoeIv][start]"`date '+%Y-%m-%d %H:%M:%S'`
python calWoeIv.py "$partCount" "$startCol" "$endCol" "$datafilename" "$resultfilename"
echo "[calWoeIv][end]"`date '+%Y-%m-%d %H:%M:%S'`

#由于版本问题，可能要执行下 deactivate ，然后注释上面的iv，重新执行
# deactivate
#box img
echo "[featBoxImg][start]"`date '+%Y-%m-%d %H:%M:%S'`
python featBoxImg.py "" "$startCol" "$endCol" "$datafilename"
echo "[featBoxImg][end]"`date '+%Y-%m-%d %H:%M:%S'`

#box img
echo "[featDotImg][start]"`date '+%Y-%m-%d %H:%M:%S'`
python featDotImg.py "" "$startCol" "$endCol" "$datafilename"
echo "[featDotImg][end]"`date '+%Y-%m-%d %H:%M:%S'`

# source ~/.bashrc

echo "[stop]"`date '+%Y-%m-%d %H:%M:%S'`
