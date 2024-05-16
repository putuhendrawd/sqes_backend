#!/bin/bash
# startdate=$1
# enddate=$2
startdate=20240110
enddate=20240111

### naming overwrite preventor
name=`date -d "today" --utc +'%Y%m%d'`
if [[ -e log/m-$name.log || -L log/m-$name.log ]] ; then
    i=1
    while [[ -e log/m-$name-$i.log || -L log/m-$name-$i.log ]] ; do
        let i++
    done
    name=$name-$i
fi

### run ###
nohup bash batch.sh $startdate $enddate > batch_log/$name.log 2> batch_error/$name.err &