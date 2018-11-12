package com.madhouse.dsp

import com.madhouse.dsp.avro.MediaBid
import com.madhouse.dsp.entity._
import com.madhouse.dsp.utils.Functions._
import com.madhouse.dsp.utils.{AvroUtils, JDBCSink}
import org.apache.commons.cli._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.slf4j.{Logger, LoggerFactory}
import com.madhouse.dsp.utils.ConfigReader._

/**
  * Created by Madhouse on 2017/12/25.
  */
object AuxoRequest {
  val log: Logger = LoggerFactory.getLogger(AuxoRequest.getClass)

  def main(args: Array[String]): Unit = {

    var startOffsetEarliest = false
    var outSinkConsole = false

    val opt = new Options()
    opt.addOption("e", "earliest", false, "set the start offset of kafka as earliest")
    opt.addOption("h", "help", false, "help message")
    opt.addOption("o", "out", false, "set the out sink as Console/stdout sink")

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
    if (cl.hasOption("e")) startOffsetEarliest = true
    if (cl.hasOption("h")) {
      formatter.printHelp(formatstr, opt)
      System.exit(0)
    }
    if (cl.hasOption("o")) outSinkConsole = true

    log.info(s"#####startOffsetEarliest = $startOffsetEarliest, outSinkConsole = $outSinkConsole")

    val spark = SparkSession.builder.appName("AuxoRequest").getOrCreate()
    import spark.implicits._

    val startingOffsets = if (startOffsetEarliest) "earliest" else requestStartOffset
    val ds = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("subscribe", requestTopic)
      .option("startingOffsets", startingOffsets)
      .option("maxOffsetsPerTrigger", requestMaxOffsetPerTrigger)
      .load()
      .select("value")
      .as[Array[Byte]]

    val requestDs = ds.map(r => {
      val t = AvroUtils.decode(r, MediaBid.SCHEMA$).asInstanceOf[MediaBid]
      try {
        val (projectId, campaignId, creativeId) =
          if (t.getResponse != null) (t.getResponse.getProjectid.toInt,
            t.getResponse.getCid.toInt, t.getResponse.getCrid.toInt)
          else (0, 0, 0)
        RequestReport(timeprocess(t.getTime.toLong, 1800L), projectId,
          campaignId, creativeId, t.getRequest.getMediaid.toInt,
          t.getRequest.getAdspaceid.toInt, requesStatus(t.getStatus))
      } catch {
        case e: Throwable => log.info(s"#####get information from ${t.toString}, exception happened:$e")
          RequestReport(timeprocess(t.getTime.toLong, 1800L), 0, 0, 0, 0, 0, requesStatus(0))
      }
    }).select("timestamp", "projectId", "campaignId", "creativeId", "mediaId", "adSpaceId", "reqs", "bids", "wins", "errs")
      .coalesce(8)

    if (outSinkConsole) {
      requestDs.writeStream
        .queryName(s"RTReport-Request-Query")
        .outputMode("append")
        .format("console")
        .trigger(Trigger.ProcessingTime(requestTriggerTimeMS))
        .start()
        .awaitTermination()
    } else {
      val jdbcConf = JDBCConf(mysqlUrl, mysqlUser, mysqlPasswd, mysqlBatchSize, requestTable)
      log.info(s"#####jdbcConf = ${jdbcConf.toString}")
      requestDs.writeStream
        .queryName(s"RTReport-Request-Query")
        .outputMode("append")
        .foreach(new JDBCSink("request", jdbcConf))
        .option("checkpointLocation", s"/travelmad/spark/checkpoint/$requestTopic")
        .trigger(Trigger.ProcessingTime(requestTriggerTimeMS))
        .start()
        .awaitTermination()
    }

  }
}
