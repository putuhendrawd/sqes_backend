#!/bin/bash
#################### basic input ####################
date=20240306
echo "################ $date #################"
#################### naming #########################
name="$date"
if [[ -e log/m-$name.log || -L log/m-$name.log ]] ; then
    i=1
    while [[ -e log/m-$name-$i.log || -L log/m-$name-$i.log ]] ; do
        let i++
    done
    name=$name-$i
fi
#################### running #########################
# nohup python3 sqes_v3_processing.py $date verbose > log/$name.log 2> error/$wname.err &
nohup timeout -14400 python3 bin/sqes_v3_multiprocessing.py $date verbose > log/m-$name.log 2> error/m-$name.err &
