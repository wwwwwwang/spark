package com.datageek.etl

/**
  * Created by SNOW on 2017/3/23.
  */

import etlutil._
import kafka.serializer.StringDecoder
import org.apache.commons.cli._
import org.apache.commons.logging.LogFactory
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.elasticsearch.spark.rdd.EsSpark

import scala.collection.JavaConverters._
import scala.collection.mutable

@deprecated
object ETLWithKey1 {
  private val log = LogFactory.getLog(ETLWithKey1.getClass)
  private var partitions = 3
  private var interval = 5
  private var reset_kafka = false
  private var jobName = ""
  private var useDB = true
  private var useFunction = false
  private var hasRawMsg = true
  private var outToEs = false
  private var outToHive = false
  private var executorMemory = ""
  private var executorCores = ""
  private var totalExecutorCores = ""
  var topKeys = ""
  var needReGet = true

  def main(args: Array[String]): Unit = {

    val opt = new Options()
    opt.addOption("b", "kafka-brokers", true, "the brokers list of kafka")
    opt.addOption("c", "contains-rawmsg", false, "whether the result contains to raw message")
    opt.addOption("d", "dry-run", false, "whether execute this process with dry_run")
    opt.addOption("f", "funciton-using", false, "whether use function to change regexp group value")
    opt.addOption("g", "giveup-database", false, "whether give up using mysql database as storage of all configurations")
    opt.addOption("h", "help", false, "help message")
    opt.addOption("i", "interval", true, "the batch interval of streaming")
    opt.addOption("k", "kafka-topic", true, "the topic of kafka")
    opt.addOption("n", "name", true, "the job unique name(id)")
    opt.addOption("p", "partitions", true, "the number of partitions")
    opt.addOption("r", "reset-kafka-offset", false, "consume kafka topic from beginning")
    opt.addOption("s", "save-name", true, "the save table name(or index/type) of log")
    opt.addOption("t", "type", true, "the device type of log")
    opt.addOption("em", "executor-memory", true, "executor-memory will be set by sparkConf")
    opt.addOption("ec", "executor-cores", true, "executor-cores will be set by sparkConf")
    opt.addOption("tec", "total-executor-cores", true, "total-executor-cores will be set by sparkConf")
    opt.addOption("oe", "out-to-es", false, "put out the result to es")
    opt.addOption("oh", "out-to-hive", false, "put out the result to hive")
    opt.addOption("ut", "update-time", true, "the value will be set to thread to update broadcast variable, unit: second")

    var dryRun = false
    var topicName = ""
    var brokers = ""
    var updateInterval: Long = 30

    val formatstr = "sh run.sh yarn-cluster|yarn-client|local ...."
    val formatter = new HelpFormatter
    val parser = new PosixParser

    var cl: CommandLine = null
    try {
      cl = parser.parse(opt, args)
    }
    catch {
      case e: ParseException =>
        e.printStackTrace()
        formatter.printHelp(formatstr, opt)
        System.exit(1)
    }
    if (cl.hasOption("b")) brokers = cl.getOptionValue("b")
    if (cl.hasOption("c")) hasRawMsg = false
    if (cl.hasOption("d")) dryRun = true
    if (cl.hasOption("f")) useFunction = true
    if (cl.hasOption("g")) useDB = false
    if (cl.hasOption("h")) {
      formatter.printHelp(formatstr, opt)
      System.exit(0)
    }
    if (cl.hasOption("i")) interval = cl.getOptionValue("i").toInt
    if (cl.hasOption("k")) topicName = cl.getOptionValue("k")
    if (cl.hasOption("n")) jobName = cl.getOptionValue("n")
    if (cl.hasOption("oe")) outToEs = true
    if (cl.hasOption("oh")) outToHive = true
    if (cl.hasOption("p")) partitions = cl.getOptionValue("p").toInt
    if (cl.hasOption("r")) reset_kafka = true
    if (cl.hasOption("em")) executorMemory = cl.getOptionValue("em")
    if (cl.hasOption("ec")) executorCores = cl.getOptionValue("ec")
    if (cl.hasOption("tec")) totalExecutorCores = cl.getOptionValue("tec")
    if (cl.hasOption("ut")) updateInterval = cl.getOptionValue("ut").toLong

    if (updateInterval != 30)
      Timer.length = updateInterval

    log.info(s"####updateInterval: $updateInterval")
    log.info(s"####dryRun: $dryRun")
    log.info(s"####useDB: $useDB")
    log.info(s"####useFunction: $useFunction")
    log.info(s"####hasRawMsg: $hasRawMsg")
    log.info(s"####topKeys: $topKeys")
    log.info(s"####brokers quorum: $brokers")
    log.info(s"####partitions: $partitions")
    log.info(s"####reset_kafka: $reset_kafka")
    log.info(s"####job_name: $jobName")
    log.info(s"####outToEs: $outToEs")
    log.info(s"####outToHive: $outToHive")
    log.info(s"####executorMemory: $executorMemory")
    log.info(s"####executorCores: $executorCores")
    log.info(s"####totalExecutorCores: $totalExecutorCores")

    new Thread(new ThreadDemo("ETLWithKey1", topicName)).start()
    process(topicName, brokers, dryRun)
  }

  def process(topics: String, brokers: String, dryRun: Boolean): Unit = {

    val sparkConf = new SparkConf().setAppName("etl_w_" + topics)
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    if (executorMemory.equalsIgnoreCase("")
      || executorCores.equalsIgnoreCase("")
      || totalExecutorCores.equalsIgnoreCase("")) {
      log.info(s"####one of executorMemory/executorCores/totalExecutorCores does not has right value, use config value in spark_config.cfg as default!")
    } else {
      sparkConf.set("spark.executor.memory", executorMemory+"G")
      sparkConf.set("spark.executor.cores", executorCores)
      sparkConf.set("spark.cores.max", totalExecutorCores)
    }

    val ssc = new StreamingContext(sparkConf, Seconds(interval))

    val mysqljdbcDao = new MysqlJDBCDao2
    var esInfo = mutable.Map[String, String]()
    val pr = new PropertiesReader
    pr.loadPropertiesByClassPath("mysql.properties")
    if (outToEs) {
      if (useDB) {
        esInfo = mysqljdbcDao.getESInfo.asScala
      } else {
        //pr.loadProperties("config/mysql.properties")
        val node = pr.getProperty("es.node")
        val port = pr.getProperty("es.port")
        esInfo += ("es_nodes" -> (if (!node.equalsIgnoreCase("")) node else "localhost"))
        esInfo += ("es_port" -> (if (!port.equalsIgnoreCase("")) port else "9092"))
      }
    }
    //topKeys = pr.getProperty("topic.keys")
    log.info(s"####es.nodes = ${esInfo.get("es_nodes")}")
    log.info(s"####es.port = ${esInfo.get("es_port")}")

    val bUseDB = ssc.sparkContext.broadcast(useDB)
    val bUseFunction = ssc.sparkContext.broadcast(useFunction)
    val bHasRawMsg = ssc.sparkContext.broadcast(hasRawMsg)
    val bTopicKeys = BroadcastWrapper[String](ssc, topKeys)
    val bNeedReGet = BroadcastWrapper[Boolean](ssc, needReGet)

    val topicsSet = topics.split(",").toSet
    var kafkaParams = Map[String, String]("metadata.broker.list" -> brokers,
      "group.id" -> ("etl_w_" + topics.replaceAll(",", "_")))
    if (reset_kafka) kafkaParams += ("auto.offset.reset" -> "smallest")

    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    log.info("####applictionid = " + ssc.sparkContext.applicationId)

    mysqljdbcDao.closeConn()

    val maps = messages.repartition(partitions)
      .map(m => (m._1, Extract.log2json(m._2, m._1, bUseDB.value, bHasRawMsg.value, bUseFunction.value, bNeedReGet.value)))
      .cache

    maps.foreachRDD(rdd => {
      bTopicKeys.update(topKeys, blocking = true)
      bNeedReGet.update(needReGet, blocking = true)
      needReGet = false
      log.info(s"####bTopicKeys: ${bTopicKeys.value}")
      log.info(s"####bNeedReGet: ${bNeedReGet.value}")
      if(!bTopicKeys.value.trim.equalsIgnoreCase("")){
        val keys = bTopicKeys.value.split(",")
        log.info(s"####keys.size: ${keys.size}")
        for (key <- keys) {
          val keyrdd = rdd.filter(e => e._1.equalsIgnoreCase(key)).map(_._2)
          if (!keyrdd.isEmpty()) {
            if (outToEs) {
              val esOption = Map[String, String]("pushdown" -> "true",
                "es.index.auto.create" -> "true",
                "es.nodes" -> esInfo.getOrElseUpdate("es_nodes", "172.31.18.15"), //"172.31.18.10",
                "es.port" -> esInfo.getOrElseUpdate("es_port", "9092") //"9292"
                //"es.net.http.auth.user"->"elastic",
                //"es.net.http.auth.pass"->"elastic_datageek"
              )
              //EsSpark.saveToEs(rdd,"spark/test4rdd",esOption)
              EsSpark.saveJsonToEs(keyrdd, key + "/system", esOption) //"spark4rdd/test"
              log.info(s"####saving json rdd to es finished...")
            }
            if (outToHive) {
              val sparkSession = SparkSession.builder
                .config(sparkConf) //.config("spark.sql.warehouse.dir", "/user/hive/warehouse")
                .enableHiveSupport().getOrCreate()
              val df = sparkSession.read.json(keyrdd)
              log.info(s"####df.partitionNumber = ${df.rdd.getNumPartitions}")
              val count = df.count()
              log.info(s"####df.count = $count")
              if (count > 0) {
                log.info(s"####start to save df in hive table $key")
                //df.write.mode("append").format("orc").partitionBy("date","threadNo").saveAsTable(tableName)
                df.write.mode("append").format("orc").saveAsTable(key)
                log.info(s"####save to hive table finished!")
              } else {
                log.info(s"####df is emtpy!")
              }
            }
          } else {
            log.info(s"####the keyrdd of $key in topic $topicsSet from ETL is empty! ")
          }
        }
      }else{
        log.info(s"####the keys in topic $topicsSet is empty!!")
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
