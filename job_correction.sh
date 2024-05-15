#!/bin/bash
#################### basic input ####################
#wkt1=`date -d "1 day ago" --utc +'%Y%m%d'`
#wkt2=`date -d "3 day ago" --utc +'%Y-%m-%d'`
wkt1=20240203
echo "################ $wkt1 #################"
#################### naming #########################
name="$wkt1"
if [[ -e log/m-$name.log || -L log/m-$name.log ]] ; then
    i=1
    while [[ -e log/m-$name-$i.log || -L log/m-$name-$i.log ]] ; do
        let i++
    done
    name=$name-$i
fi
#################### running #########################
# nohup python3 sqes_v3_processing.py $wkt1 verbose > log/$name.log 2> error/$wname.err &
nohup python3 sqes_v3_multiprocessing.py $wkt1 verbose > log/m-$name.log 2> error/m-$name.err &
