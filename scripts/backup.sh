
date_time=`date +%Y%m%d`
rm -rf ./etcd_backup/
etcdutl backup --data-dir ../default.etcd --backup-dir ./etcd_backup/"${date_time}"

find ./etcd_backup/ -ctime +7 -exec rm -r {} \;
