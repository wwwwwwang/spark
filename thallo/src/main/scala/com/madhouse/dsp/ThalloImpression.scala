package com.madhouse.dsp

import com.madhouse.dsp.avro.ImpressionTrack
import com.madhouse.dsp.utils.AvroUtils
import com.madhouse.dsp.utils.ConfigReader._
import com.madhouse.dsp.utils.Functions._
import com.madhouse.dsp.utils.OffsetUtils._
import kafka.message.MessageAndMetadata
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.commons.cli._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.{Logger, LoggerFactory}

/**
  * Created by Madhouse on 2017/12/25.
  */
object ThalloImpression {
  val log: Logger = LoggerFactory.getLogger(ThalloImpression.getClass)

  def main(args: Array[String]): Unit = {
    var offset = "zk"

    val opt = new Options()
    opt.addOption("h", "help", false, "help message")
    opt.addOption("o", "offset", true, "set the offset of kafka: earliest, latest, zk")

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

    log.info(s"#####offset = $offset")

    val sparkConf = new SparkConf().setAppName("ThalloImpression")
      .set("spark.streaming.kafka.maxRatePerPartition", s"$impMaxRatePerPartition")
    val ssc = new StreamingContext(sparkConf, Seconds(impStreamingInterval))

    val topicsSet = impTopic.split(",").toSet
    var kafkaParams = Map[String, String](
      "metadata.broker.list" -> bootstrapServers,
      "group.id" -> ("ThalloImp_" + impTopic.replaceAll(",", "_"))
    )

    val fromOffsets = getOffset("imp", offset)

    val messages = if (fromOffsets.nonEmpty) {
      for (o <- fromOffsets) {
        log.info(s"##### topic:${o._1.topic}, partition:${o._1.partition}, offset:${o._2}")
      }
      KafkaUtils.createDirectStream[String, Array[Byte], StringDecoder, DefaultDecoder, (String, Array[Byte])](
        ssc, kafkaParams, fromOffsets, (mmd: MessageAndMetadata[String, Array[Byte]]) => (mmd.key, mmd.message))
    } else {
      log.info("##### the offset is not generated in zookeeper....")
      if (offset.equalsIgnoreCase("earliest"))
        kafkaParams += ("auto.offset.reset" -> "smallest")
      KafkaUtils.createDirectStream[String, Array[Byte], StringDecoder, DefaultDecoder](ssc, kafkaParams, topicsSet)
    }

    var offsetRanges = Array[OffsetRange]()
    val logBeforeDecoded = messages.transform(rdd => {
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd.map(r => r._2)
    })

    logBeforeDecoded.repartition(2).foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        val sparkSession = SparkSession.builder
          .config(sparkConf).getOrCreate()
        import sparkSession.implicits._
        val jsonDs = rdd.map(r => {
          val a = AvroUtils.decode(r, ImpressionTrack.SCHEMA$).asInstanceOf[ImpressionTrack]
          val ts = timeprocess(a.getTime, 60)
          (ts, a.toString)
        }).toDS().cache()
        val tss = jsonDs.map(r => r._1).distinct().collect()
        log.info(s"##### there are ${tss.length} timestamps in current dataset...")
        for (ts <- tss) {
          val df = sparkSession.read.json(jsonDs.filter(r => r._1 == ts).map(r => r._2))
          val path = hdfsBasePath + "/" + impTopic + "/" + ts
          log.info(s"##### start to write df of logs to hdfs, path is $path")
          df.coalesce(1).write.format("parquet").mode("append").save(path)
          log.info(s"##### saving df of $impTopic logs to hdfs finished...")
        }
        jsonDs.unpersist()
        log.info("##### start to write offset to zookeeper...")
        writeOffset("imp", offsetRanges)
        log.info("##### writing offset to zookeeper finished...")
      } else {
        showOffset("imp", offsetRanges)
        log.info(s"##### rdd of $impTopic logs from kafka is empty, and no needing update offset in zookeeper...")
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
