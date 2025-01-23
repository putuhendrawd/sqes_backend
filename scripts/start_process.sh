#!/bin/bash
#################### basic input ####################
date=`date -d "1 day ago" --utc +'%Y%m%d'`
del_date=`date -d "3 day ago" --utc +'%Y-%m-%d'`
CONFIG_FILE="../config/config.ini"
SQES_PYTHON_PATH=$(awk -F= '/^python_path/ {print $2}' $CONFIG_FILE | tr -d '[:space:]')
SQES_STORAGE_PATH=$(awk -F= '/^storage_path/ {print $2}' $CONFIG_FILE | tr -d '[:space:]')
rm -r -f $SQES_STORAGE_PATH/$del_date
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
nohup $SQES_PYTHON_PATH ../sqes_backend/sqes_multiprocessing.py $date verbose > ../logs/log/$name.log 2> ../logs/error/$name.err &
