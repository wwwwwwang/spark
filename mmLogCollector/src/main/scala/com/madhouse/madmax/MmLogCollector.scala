package com.madhouse.madmax

import com.madhouse.madmax.avro.{ClickTrack, ImpressionTrack, MediaBid, WinNotice}
import com.madhouse.madmax.utils.AvroUtils
import kafka.message.MessageAndMetadata
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.commons.cli._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.{Logger, LoggerFactory}
import com.madhouse.madmax.utils.Constant._
import com.madhouse.madmax.utils.ConfigReader._
import com.madhouse.madmax.utils.Functions._
import com.madhouse.madmax.utils.OffsetUtils._

/**
  * Created by Madhouse on 2017/12/25.
  */
object MmLogCollector {
  //val log: Logger = LoggerFactory.getLogger(ThalloRequest.getClass)

  def main(args: Array[String]): Unit = {
    var offset = ZK
    var logType = WIN

    val opt = new Options()
    opt.addOption("h", "help", false, "help message")
    opt.addOption("o", "offset", true, "set the offset of kafka: earliest, latest, zk")
    opt.addOption("t", "type", true, "the type of log: req, imp, clk, win")

    val formatstr = "sh run.sh mesos ...."
    val formatter = new HelpFormatter
    val parser = new PosixParser

    var cl: CommandLine = null
    try
      cl = parser.parse(opt, args)
    catch {
      case e: ParseException =>
        e.printStackTrace()
        formatter.printHelp(formatstr, opt)
        System.exit(1)
    }
    if (cl.hasOption("h")) {
      formatter.printHelp(formatstr, opt)
      System.exit(0)
    }
    if (cl.hasOption("o")) offset = cl.getOptionValue("o")
    if (cl.hasOption("t")) logType = cl.getOptionValue("t")

    log(s"#####logType = $logType, offset = $offset")

    val (maxRatePerPartition, interval, topics, startOffsetWithoutZk, partitions) = logType match {
      case REQ => (reqMaxRatePerPartition, reqStreamingInterval, reqTopic, reqStartOffset, reqPartitions)
      case IMP => (impMaxRatePerPartition, impStreamingInterval, impTopic, impStartOffset, impPartitions)
      case CLK => (clkMaxRatePerPartition, clkStreamingInterval, clkTopic, clkStartOffset, clkPartitions)
      case WIN => (winMaxRatePerPartition, winStreamingInterval, winTopic, winStartOffset, winPartitions)
    }

    val sparkConf = new SparkConf().setAppName(s"MmLogCollector_$logType")
      .set("spark.streaming.kafka.maxRatePerPartition", s"$maxRatePerPartition")
    val ssc = new StreamingContext(sparkConf, Seconds(interval))

    val topicsSet = topics.split(",").toSet
    var kafkaParams = Map[String, String](
      "metadata.broker.list" -> bootstrapServers,
      "group.id" -> (s"Log_collector_" + topics.replaceAll(",", "_"))
    )

    val fromOffsets = getOffset(logType, offset)

    val messages = if (fromOffsets.nonEmpty) {
      for (o <- fromOffsets) {
        log(s"##### topic:${o._1.topic}, partition:${o._1.partition}, offset:${o._2}")
      }
      KafkaUtils.createDirectStream[String, Array[Byte], StringDecoder, DefaultDecoder, (String, Array[Byte])](
        ssc, kafkaParams, fromOffsets, (mmd: MessageAndMetadata[String, Array[Byte]]) => (mmd.key, mmd.message))
    } else {
      log("##### the offset is not generated in zookeeper....")
      if (startOffsetWithoutZk.equalsIgnoreCase(EARLIEST))
        kafkaParams += ("auto.offset.reset" -> "smallest")
      KafkaUtils.createDirectStream[String, Array[Byte], StringDecoder, DefaultDecoder](ssc, kafkaParams, topicsSet)
    }

    var offsetRanges = Array[OffsetRange]()
    val logBeforeDecoded = messages.transform(rdd => {
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd.map(r => r._2)
    })

    logBeforeDecoded.repartition(partitions).foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        val sparkSession = SparkSession.builder
          .config(sparkConf).getOrCreate()
        import sparkSession.implicits._
        val jsonDs = rdd.map(r => {
          logType match {
            case REQ => val a = AvroUtils.decode(r, MediaBid.SCHEMA$).asInstanceOf[MediaBid]
              val ts = timeprocess(a.getTs, HALFHOUR)
              val jsonString =
                if (a.getResponse == null) a.toString.replaceAll(", \"response\": null", "") else a.toString
              (ts, jsonString)
            case IMP => val a = AvroUtils.decode(r, ImpressionTrack.SCHEMA$).asInstanceOf[ImpressionTrack]
              val ts = timeprocess(a.getTs, HALFHOUR)
              (ts, a.toString)
            case CLK => val a = AvroUtils.decode(r, ClickTrack.SCHEMA$).asInstanceOf[ClickTrack]
              val ts = timeprocess(a.getTs, HALFHOUR)
              (ts, a.toString)
            case WIN => val a = AvroUtils.decode(r, WinNotice.SCHEMA$).asInstanceOf[WinNotice]
              val ts = timeprocess(a.getTs, HALFHOUR)
              (ts, a.toString)
          }
        }).toDS().cache()
        val tss = jsonDs.map(_._1).distinct().collect()
        log(s"##### there are ${tss.length} timestamps in current dataset...")
        for (ts <- tss) {
          val df = sparkSession.read.json(jsonDs.filter(r => r._1 == ts).map(r => r._2))
          val dateTime = mkPath(ts)
          val path = s"$hdfsBasePath/$topics/$dateTime"
          log(s"##### start to write df of logs to hdfs, path is $path")
          df.coalesce(partitions).write.format(FORMAT).mode(MODE).save(path)
          log(s"##### saving df of $topics logs to hdfs finished...")
        }
        jsonDs.unpersist()
        log("##### start to write offset to zookeeper...")
        writeOffset(logType, offsetRanges)
        log("##### writing offset to zookeeper finished...")
      } else {
        showOffset(logType, offsetRanges)
        log(s"##### rdd of $topics logs from kafka is empty, and no needing update offset in zookeeper...")
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
