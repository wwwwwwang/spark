#!/bin/sh
#
cd `dirname $0`
basepath=$(cd `dirname $0`; pwd)
#echo $basepath

###mkdir for elt logs path start######
logpath=$basepath"/logs"
if [ ! -d "$logpath" ]; then
 mkdir "$logpath"
else
 echo "$logpath"" is exist!!"
fi
###mdkir end##########################

###add classpath######################
export CLASSPATH=$CLASSPATH:.:$basepath/config:$basepath/lib
#sh run.sh standalone 2 -b 172.31.18.10:9092 -i 5 -k topicWithKey -kk el,el1 -n uuid1 -t el,el1 -s es_el/test,es_el1/test -g -r -oe
###end################################

######check kafka status##############
i=1;
for ((i=1; i<4; i++)); do
  kafkaStatus=`jps | grep Kafka`
  echo "kafkaStatus=$kafkaStatus"
  if [[ "x$kafkaStatus" != "x" ]]; then
    echo "kafka is active, ready to submit etl job to spark...."
    break
  else
    echo "i=$i"
    if [[ $i<4 ]]; then
      echo "kafka is down, try to restart...."
      #boot kafka
      kafka_home=`sed '/^kafka_home=/!d;s/.*=//' spark_config.cfg`
      kafka_serverlog=`sed '/^kafka_serverlog=/!d;s/.*=//' spark_config.cfg`
      kafka_server_log=${kafka_serverlog%/*}
      if [ ! -d "$kafka_server_log" ]; then
        mkdir "$kafka_server_log"
      else
        echo "$kafka_server_log is exist!!"
      fi
      #echo "${kafka_home}/bin/kafka-server-start.sh ${kafka_home}/config/server.properties > ${kafka_serverlog} &"
      eval "${kafka_home}/bin/kafka-server-start.sh ${kafka_home}/config/server.properties > ${kafka_serverlog} &"
      sleep 3
    fi
  fi
done
if [[ $i>=3 ]]; then
  echo "restart kafka failed 3 times, exit...."
  exit 1
fi
echo "checking kafka status finishes...."
###check end##########################

###etl job submit#####################
classNo=4
if [[ $# > 1 ]];then
classNo=$2
fi
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
sh run.sh standalone $classNo "$option" > logs/"$type"_etl_with_keys.log 2>&1
###etl job submit end##################

###get driver id of application########
driverID=`sed '/submissionId/!d;s/^.*: "//;s/",//' logs/${type}_etl_with_keys.log`
date=`date "+%Y-%m-%d %H:%M:%S"`

if [[ "x$uuid" != "x" && "X$driverID" != "x" ]]; then
  echo "uuid="$uuid", driverid="$driverID", date="$date
  echo $uuid","$driverID","$date > logs/formysql.log
fi
###save driverid into log file end#####

cd -
