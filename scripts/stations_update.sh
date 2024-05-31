#!/bin/bash
#################### basic input ####################
date=`date -d "today" --utc +'%Y%m%d'`
echo "################ $date #################"
#################### naming #########################
name="$date"
if [[ -e ../logs/../logs/log/stations_update-$name.log || -L ../logs/log/stations_update-$name.log ]] ; then
    i=1
    while [[ -e ../logs/log/stations_update-$name-$i.log || -L ../logs/log/stations_update-$name-$i.log ]] ; do
        let i++
    done
    name=$name-$i
fi
#################### running #########################
# nohup python3 sqes_v3_processing.py $date verbose > ../logs/log/$name.log 2> error/$wname.err &
nohup timeout 1h python3 ../sqes_backend/utils/stations_update.py > ../logs/log/stations_update-$name.log 2> ../logs/error/stations_update-$name.err &
