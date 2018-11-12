#!/bin/bash
dt=`date -d '-30 minute' +'%Y%m%d%H:%M'`
m=00
if [ ${dt#*:} -gt 30 ]; then
  m=30
fi
r=${dt%:*}$m

if [ $# -eq 0 ]; then
 s=$r
 e=$r
elif [ $# -eq 1 ]; then
 s=$1
 e=$1
elif [ $# -eq 2 ]; then
 s=$1
 e=$2
fi

export JAVA_HOME=/usr/local/jdk1.8.0_131
/usr/local/spark/bin/spark-submit \
  --executor-memory 2g \
  --driver-memory 1g \
  --driver-cores 1 \
  --deploy-mode cluster \
  --master mesos://172.16.25.95:7077 \
  --conf "spark.executor.cores=1" \
  --conf "spark.cores.max=1" \
  --conf "spark.mesos.task.labels=name:$1" \
  --conf "spark.streaming.stopGracefullyOnShutdown=true" \
  --conf "spark.executor.logs.rolling.strategy=time" \
  --conf "spark.executor.logs.rolling.maxRetainedFiles=30" \
  --conf "spark.executor.uri=hdfs://madssp/madmax/spark/spark-2.2.0-bin-hadoop2.7.tgz" \
  --class com.madhouse.madmax.MmReport \
  --verbose \
  hdfs://madssp/madmax/apps/mmReport/mmReport-assembly-0.0.1.jar -s $s -e $e
