#!/bin/sh

cd `dirname $0`
sparkpath=`sed '/^sparkpath=/!d;s/.*=//' spark_config.cfg`
sparkmaster=`sed '/^samaster=/!d;s/.*=//' spark_config.cfg`
type=$1
#declare -l type
type="$(echo $type | tr '[:upper:]' '[:lower:]')"
driverid=$2
case $type in
  kill)
    sed -i "/^.*$driverid/d" logs/formysql.log
    echo "delete the line contains $driverid in logs/formysql.log"
    ;;
  status)
    ;;
  *)
    echo "\$1 is not a right value: kill or status"
    exit 1
    ;;
esac
command="${sparkpath}/bin/spark-submit --master ${sparkmaster} --$type $driverid"
echo  "command=""$command"
eval "$command"
