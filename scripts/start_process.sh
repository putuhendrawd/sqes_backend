#!/bin/bash
#################### basic input ####################
date=20240306
echo "################ $date #################"
#################### naming #########################
name="$date"
if [[ -e ../logs/log/$name.log || -L ../logs/log/$name.log ]] ; then
    i=1
    while [[ -e ../logs/log/$name-$i.log || -L ../logs/log/$name-$i.log ]] ; do
        let i++
    done
    name=$name-$i
fi
#################### running #########################
# nohup python3 sqes_v3_processing.py $date verbose > log/$name.log 2> error/$wname.err &
nohup timeout 4h python3 ../sqes_backend/sqes_multiprocessing.py $date verbose > ../logs/log/$name.log 2> ../logs/error/$name.err &
