#!/bin/bash
export JAVA_HOME=/usr/local/jdk1.8.0_131

if [ "$#" != "1" ]; then
    echo "usage: $0 < req | clk | imp | win >(ignore case)"
    exit 1
fi

type=$1
type="$(echo $type | tr '[:upper:]' '[:lower:]')"

echo "type=$type"

/usr/local/spark/bin/spark-submit \
  --executor-memory 1g \
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
  --class com.madhouse.madmax.MmReportRT \
  --verbose \
  hdfs://madssp/madmax/apps/mmReportRT/mmReportRT-assembly-0.0.1.jar -t $type
