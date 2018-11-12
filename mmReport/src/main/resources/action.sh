#!/bin/sh

cd `dirname $0`
sparkpath="/usr/local/spark"
sparkmaster="mesos://172.16.25.95:7077"
type=$1
#declare -l type
type="$(echo $type | tr '[:upper:]' '[:lower:]')"
driverid=$2
case $type in
  kill | status)
    ;;
  *)
    echo "\$1 is not a right value: kill or status"
    exit 1
    ;;
esac
command="${sparkpath}/bin/spark-submit --master ${sparkmaster} --$type $driverid"
echo  "command=""$command"
eval "$command"
