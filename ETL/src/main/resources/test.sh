cd `dirname $0`
basepath=$(cd `dirname $0`; pwd)
echo $basepath
export CLASSPATH=$CLASSPATH:.:$basepath/config
#echo "\$1=""$1"", \$2=""$2" > parameter.log
#sh run.sh yarn-cluster $1 $2
#sh run.sh standalone 0 -b 172.31.18.14:9092,172.31.18.13:9092,172.31.18.12:9092 -i 10 -k foo -n uuid1 -t eventlog -s jiangyin.eventlog_test -r
#sh run.sh standalone 0 -b 172.31.18.10:9092 -i 10 -k eventlog -n uuid1 -t eventlog -s jiangyin.eventlog_test -r
#sh run.sh local 0 -b 172.31.18.10:9092 -i 5 -k eventlog -n uuid1 -t eventlog -s es/spark_test -o es -g -r 
#sh run.sh standalone 0 -b 172.31.18.10:9092 -i 5 -k eventlog -n uuid1 -t eventlog -s es/spark_test -o es -g -r 
#sh run.sh local 0 -b 172.31.18.10:9092 -i 10 -k eventlog -n uuid1 -t eventlog -s es/spark_test -o es -g -r 
#sh run.sh standalone 2 -b 172.31.18.10:9092 -i 5 -k topicWithKey -n uuid1 -t el,el1 -s es_el/test,es_el1/test -g -r -oe 
#sh run.sh standalone 3 -b 172.31.18.10:9092 -i 5 -k topicWithKey -n uuid1 -g -r -oe 
sh run.sh standalone 5 -b 172.31.18.10:9092 -i 5 -k jsontest -n 186974d2-3957-41c2-b337-e7d0d98995f8 -oe -em 3 -ec 2 -tec 2
cd -
