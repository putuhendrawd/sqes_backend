#!/bin/bash
# startdate=$1
# enddate=$2
startdate=20241215
enddate=20241215

### naming overwrite preventor
name=`date -d "today" --utc +'%Y%m%d'`
if [[ -e ../logs/batch_log/$name.log || -L ../logs/batch_log/$name.log ]] ; then
    i=1
    while [[ -e ../logs/batch_log/$name-$i.log || -L ../logs/batch_log/$name-$i.log ]] ; do
        let i++
    done
    name=$name-$i
fi

### run ###
nohup bash batch.sh $startdate $enddate > ../logs/batch_log/$name.log 2> ../logs/batch_error/$name.err &