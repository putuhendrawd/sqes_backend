#!/bin/bash
#wkt1=`date -d "1 day ago" --utc +'%Y%m%d'`
#wkt2=`date -d "3 day ago" --utc +'%Y-%m-%d'`
wkt1=20240405
echo "################ $wkt1 #################"
# nohup python3 sqes_v3_processing.py $wkt1 verbose > log/$wkt1.log 2> error/$wkt1.err &
nohup python3 sqes_v3_multiprocessing.py $wkt1 verbose > log/m-$wkt1.log 2> error/m-$wkt1.err &
