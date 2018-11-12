cd `dirname $0`
basepath=$(cd `dirname $0`; pwd)
echo $basepath
export CLASSPATH=$CLASSPATH:.:$basepath/config:$basepath/lib
#sh run.sh standalone 0 -b 172.31.18.10:9092 -i 5 -k eventlog -n uuid1 -t eventlog -s es/spark_test -o es -g -r 
#sh run.sh local 0 -b 172.31.18.10:9092 -i 10 -k eventlog -n uuid1 -t eventlog -s es/spark_test -o es -g -r 
option=$1
option=${option//@2@/ }
type=${option#*-t }
type=${type%% *}
echo "type="$type
uuid=${option#*-n }
uuid=${uuid%% *}
echo "option"=$option >> logs/etl.log
#echo "sh run.sh standalone 0 ""$option"
#sh run.sh standalone 0 "$option" 
sh run.sh standalone 0 "$option" > logs/"$type"_etl.log 2>&1

driverID=`sed '/submissionId/!d;s/^.*: "//;s/",//' logs/${type}_etl.log`
date=`date "+%Y-%m-%d %H:%M:%S"`

if [[ "x$uuid" != "x" && "X$driverID" != "x" ]]; then
  echo "uuid="$uuid", driverid="$driverID", date="$date
  echo $uuid","$driverID","$date > logs/formysql.log
fi

cd -
