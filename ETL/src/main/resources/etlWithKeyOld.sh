cd `dirname $0`
basepath=$(cd `dirname $0`; pwd)
echo $basepath
logpath=$basepath"/logs"
if [ ! -d "$logpath" ]; then
 mkdir "$logpath"
else
 echo "$logpath"" is exist!!"
fi
export CLASSPATH=$CLASSPATH:.:$basepath/config:$basepath/lib
#sh run.sh standalone 2 -b 172.31.18.10:9092 -i 5 -k topicWithKey -kk el,el1 -n uuid1 -t el,el1 -s es_el/test,es_el1/test -g -r -oe
option=$1
option=${option//@2@/ }
type=${option#*-k }
type=${type%% *}
type=${type//,/_}
echo "type="$type
uuid=${option#*-n }
uuid=${uuid%% *}
echo "option"=$option >> logs/etl_with_keys.log
#echo "sh run.sh standalone 0 ""$option"
#sh run.sh standalone 0 "$option"
sh run.sh standalone 4 "$option" > logs/"$type"_etl_with_keys.log 2>&1

driverID=`sed '/submissionId/!d;s/^.*: "//;s/",//' logs/${type}_etl_with_keys.log`
date=`date "+%Y-%m-%d %H:%M:%S"`

if [[ "x$uuid" != "x" && "X$driverID" != "x" ]]; then
  echo "uuid="$uuid", driverid="$driverID", date="$date
  echo $uuid","$driverID","$date > logs/formysql.log
fi
cd -
