#!/bin/bash
startdate=$1
enddate=$2

echo "running batch process for sqes_v3_multiprocessing.py"
dt1="$(date +"%Y-%m-%d %H:%M:%S.%3N")"
echo "Start date and time: $dt1"
### searching for dates ###
dates=()
for (( date="$startdate"; date <= enddate; )); do
    dates+=( "$date" )
    date="$(date --date="$date + 1 days" +'%Y%m%d')"
done
echo "processing dates:"
echo "${dates[@]}"

### main loop ###
for date in ${dates[@]}
do
    ### naming overwrite preventor
    name="$date"
    if [[ -e log/m-$name.log || -L log/m-$name.log ]] ; then
        i=1
        while [[ -e log/m-$name-$i.log || -L log/m-$name-$i.log ]] ; do
            let i++
        done
        name=$name-$i
    fi
    ### running
    nohup timeout -14400 python3 sqes_v3_multiprocessing.py $date verbose > log/m-$name.log 2> error/m-$name.err &
    pid=$!
    echo "Running $date with pid: $pid"
    wait $pid
done

dt2="$(date +"%Y-%m-%d %H:%M:%S.%3N")"
echo "End date and time: $dt2"

#### timing report
starttime=$(date -d "${dt1}" +%s)
endtime=$(date -d "${dt2}" +%s)
runtime=$((endtime - starttime))

days=$((runtime / 86400))
hours=$(( (runtime % 86400) / 3600 ))
minutes=$(( (runtime % 3600) / 60 ))
seconds=$((runtime % 60))

post=$(printf "%dd %dh %dm %ds" $days $hours $minutes $seconds)
echo "runtime:"
echo $post
echo "batch process done"