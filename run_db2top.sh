#!/usr/bin/ksh

PRUNE=12
I=1



set -x

while [ true ]
do
  export tmstmp=`date +"%Y-%m-%d-%H"`
  db2top -d fh0431ab -i 5 -b l -o db2top-${tmstmp}.out -m 60
  gzip db2top-${tmstmp}.out


  I=1
  ls -1 db2top-????-??-??-??.out.gz| sort -r | while read FNAME
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

#db2top -d fh0431ab -b l -o db2top-l.out -m 2



