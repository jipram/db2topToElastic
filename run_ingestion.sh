for i in $(ls -1 data/*_db2top-*) 
do 
	echo $i 
#	echo "cat $i | gzip -cd | perl db2top2es_ng.pl $(echo $i | cut -f 2,3,4 -d -) $(echo $i | cut -f 2 -d '/' |  cut -f 1 -d _ ) "
	cat $i | gzip -cd | perl db2top2es_ng.pl $(echo $i | cut -f 2,3,4 -d -) $(echo $i | cut -f 2 -d '/' |  cut -f 1 -d _ ) 
done 

