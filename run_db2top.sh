#!/usr/bin/ksh

PRUNE=200
I=1
DB_NAME='FH0131AB'
DATA_PATH='/ap/fh/fhdba01/ufhdba01/jip/db2top/data'


set -x

while [ true ]
do
  export tmstmp=`date +"%Y-%m-%d-%H"`
  db2top -d $DB_NAME -i 5 -b l -o $DATA_PATH/${DB_NAME}_db2top-${tmstmp}.out -m 60
  gzip $DATA_PATH/${DB_NAME}_db2top-${tmstmp}.out


  I=1
  ls -1 $DATA_PATH/${DB_NAME}_db2top-????-??-??-??.out.gz| sort -r | while read FNAME
  do
    echo "FNAME je: $FNAME"
    if [[ $PRUNE -lt $I ]]
    then
      echo "mazu: $FNAME"
      rm $FNAME

    fi
    I=$(($I + 1))
  done
done

I=$(($I + 1))
  done
done
