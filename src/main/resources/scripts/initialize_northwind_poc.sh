#!/bin/bash

usage ()
{
  echo 'Usage : ./initialize_northwind_poc.sh <location_northwind_data> <output_location> <hdfs_location> <database_name>'
  exit
}

if [ "$#" -ne 4 ]
then
  usage
fi

hdfs dfs -rmr $3/northwind

rm -r $2/northwind

rm $2/northwind_partitions.hql

mkdir $2/northwind

cd $1/northwind/
echo "$1/northwind/"

cat > $2/northwind_partitions.hql << EOF1
use ${4};
EOF1

#echo "use ${hivevar:database_name};" >> $2/northwind_partitions.hql

for i in $(ls -d */);
do
  dir_name=`echo ${i%%/}`
  echo $dir_name
  if [ "${dir_name}" == "2017-10-05" ]
      then
       channel=cts
       ingest_time=1900-01-01-00
      elif [ "${dir_name}" == "2017-10-06" ]
       then
        channel=cdc
        ingest_time=2017-01-01-19
      else
       channel=cdc
       ingest_time=2017-01-01-13
  fi
  cd $1/northwind/${i%%/}
    for file in ./*.csv
     do
        arrIN=(${file//./ });
        folder_name=`echo  "${arrIN[0]}"| sed -r 's/^.{1}//'`
        echo $folder_name
        mkdir -p $2/northwind/$folder_name/edl_ingest_channel=$channel/edl_ingest_time=$ingest_time/
        cp $file $2/northwind/$folder_name/edl_ingest_channel=$channel/edl_ingest_time=$ingest_time/
        echo "ALTER TABLE stg_northwind_$folder_name ADD PARTITION (edl_ingest_channel='$channel', edl_ingest_time='$ingest_time') location 'hdfs://$3/northwind/$folder_name/edl_ingest_channel=$channel/edl_ingest_time=$ingest_time/';" >> $2/northwind_partitions.hql
     done
done



hdfs dfs -put $2/northwind $3

hive -f $2/create_stg_northwind.hql --hivevar database_name=$4

hive -f $2/northwind_partitions.hql
