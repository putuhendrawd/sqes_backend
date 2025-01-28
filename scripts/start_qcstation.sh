#!/bin/bash
#################### basic input ####################
cd "$(dirname "$0")"
date=`date -d "1 day ago" --utc +'%Y%m%d'`
# del_date=`date -d "3 day ago" --utc +'%Y-%m-%d'`
CONFIG_FILE="../config/config.ini"
SQES_PYTHON_PATH=$(awk -F= '/^python_path/ {print $2}' $CONFIG_FILE | tr -d '[:space:]')
# SQES_OUTPUTMSEED_PATH=$(awk -F= '/^outputmseed/ {print $2}' $CONFIG_FILE | tr -d '[:space:]')
# rm -r -f $SQES_OUTPUTMSEED_PATH/$del_date
#################### naming #########################
name="sqes-qcstation-$date"
if [[ -e ../logs/qcstation_log/$name.log || -L ../logs/qcstation_log/$name.log ]] ; then
    i=1
    while [[ -e ../logs/qcstation_log/$name-$i.log || -L ../logs/qcstation_log/$name-$i.log ]] ; do
        let i++
    done
    name=$name-$i
fi
#################### running #########################
nohup $SQES_PYTHON_PATH ../sqes_backend/sqes_qcstation.py > ../logs/qcstation_log/$name.log 2> ../logs/qcstation_log/$name.err &
