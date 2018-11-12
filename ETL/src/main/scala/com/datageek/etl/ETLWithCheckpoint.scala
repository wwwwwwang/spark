package com.datageek.etl

import etlutil.{Extract, MysqlJDBCDao2}
import kafka.serializer.StringDecoder
import org.apache.commons.cli._
import org.apache.commons.logging.LogFactory
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by SNOW on 2017/5/19.
  */
@deprecated
object ETLWithCheckpoint {
  private val serialVersionUID = 2L
  private val log = LogFactory.getLog(ETLWithCheckpoint.getClass)
  private var partitions = 3
  private var interval = 5
  private var reset_kafka = false
  private var jobName = ""

  def main(args: Array[String]): Unit = {

    val opt = new Options()
    opt.addOption("b", "kafka-brokers", true, "the brokers list of kafka")
    opt.addOption("d", "dry-run", false, "whether execute this process with dry_run")
    opt.addOption("h", "help", false, "help message")
    opt.addOption("i", "interval", true, "the batch interval of streaming")
    opt.addOption("k", "kafka-topic", true, "the topic of kafka")
    opt.addOption("n", "name", true, "the job unique name(id)")
    opt.addOption("p", "partitions", true, "the number of partitions")
    opt.addOption("r", "reset-kafka-offset", false, "consume kafka topic from beginning")
    opt.addOption("s", "save-name", true, "the save table name of log")
    opt.addOption("t", "type", true, "the device type of log")

    var dryRun = false
    var topicName = ""
    var brokers = ""
    var source = ""
    var tableName = ""

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
    if (cl.hasOption("d")) dryRun = true
    if (cl.hasOption("h")) {
      formatter.printHelp(formatstr, opt)
      System.exit(0)
    }
    if (cl.hasOption("i")) interval = cl.getOptionValue("i").toInt
    if (cl.hasOption("k")) topicName = cl.getOptionValue("k")
    if (cl.hasOption("n")) jobName = cl.getOptionValue("n")
    if (cl.hasOption("p")) partitions = cl.getOptionValue("p").toInt
    if (cl.hasOption("r")) reset_kafka = true
    if (cl.hasOption("t")) source = cl.getOptionValue("t")
    if (cl.hasOption("s")) tableName = cl.getOptionValue("s")

    log.info("dryRun: " + dryRun)
    log.info("topic name: " + topicName)
    log.info("brokers quorum: " + brokers)
    log.info("source type: " + source)
    log.info("partitions: " + partitions)
    log.info("reset_kafka: " + reset_kafka)
    log.info("job_name: " + jobName)
    log.info("tableName: " + tableName)

    val checkpointDirectory = "/user/spark/" + source + "/checkpoint"
    //process(topicName, brokers, source, tableName, dryRun)
    val ssc = StreamingContext.getOrCreate(checkpointDirectory,
      () => process(topicName, brokers, source, tableName, dryRun, checkpointDirectory))
    ssc.start()
    ssc.awaitTermination()
  }

  def process(topics: String, brokers: String, source: String, tableName: String, dryRun: Boolean, checkpointDirectory: String): StreamingContext = {
    val sparkConf = new SparkConf().setAppName("ETLWithKafka" + source)
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    /*sparkConf.set("pushdown", "true")
    sparkConf.set("es.nodes", "172.31.18.14")
    sparkConf.set("es.port", "9229")
    sparkConf.set("es.index.auto.create", "true")*/
    //val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sparkConf, Seconds(interval))
    ssc.checkpoint(checkpointDirectory)

    val mysqljdbcDao = new MysqlJDBCDao2
    val topicsSet = topics.split(",").toSet
    var kafkaParams = Map[String, String]("metadata.broker.list" -> brokers,
      "group.id" -> ("etl_w_" + source))
    if (reset_kafka) kafkaParams += ("auto.offset.reset" -> "smallest")

    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)
    messages.checkpoint(Seconds(2*interval))

    log.info("=========applictionid = " + ssc.sparkContext.applicationId)
    val timeStamp = System.currentTimeMillis.toString

    //mysqljdbcDao.save(jobName, ssc.sparkContext.applicationId, source, timeStamp)

    /*val sparkSession = SparkSession.builder
      .config(ssc.sparkContext.getConf) //.config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .enableHiveSupport().getOrCreate()
    import sparkSession.implicits._*/

    val lines = messages.repartition(partitions).map(_._2).map(line => {
      Extract.log2json(line, source, fromDB = false, hasMsg = true, useFunciton = false)
    })

    lines.foreachRDD(rdd => {
      val sparkSession = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)
      //if(!rdd.isEmpty()){
        val df = sparkSession.read.json(rdd)
        val count = df.count()
        log.info("-------------------------------df.count = " + count)
        //if(df.count()>0){
        if (count > 0) {
          log.info("--------------------------------start to save df in hive table " + tableName)
          /*val esOption = Map[String, String]("pushdown" -> "true",
            "es.nodes" -> "172.31.18.14", "es.port" -> "9229",
            "es.index.auto.create" -> "true")
          EsSparkSQL.saveToEs(df, "spark/test", esOption)*/
          //EsSparkSQL.saveToEs(df,"spark/test")
          df.write.mode("append").format("orc").partitionBy("date").saveAsTable(tableName)
          log.info("--------------------------------save to hive table finished!")
        } else {
          log.info("---------------------df is emtpy!")
        }
      /*}else{
        log.info("---------------------lines.rdd is emtpy!")
      }*/

    })
    ssc
  }

  object SparkSessionSingleton {

    @transient  private var instance: SparkSession = _

    def getInstance(sparkConf: SparkConf): SparkSession = {
      if (instance == null) {
        instance = SparkSession
          .builder
          .config(sparkConf)
          .enableHiveSupport()
          .getOrCreate()
      }
      instance
    }
  }

}
