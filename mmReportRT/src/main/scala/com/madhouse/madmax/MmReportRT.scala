package com.madhouse.madmax

import com.madhouse.madmax.avro.{ClickTrack, ImpressionTrack, MediaBid, WinNotice}
import com.madhouse.madmax.entity.{JDBCConf, RequestReport, TrackerReport}
import com.madhouse.madmax.utils.{AvroUtils, JDBCSink}
import org.apache.commons.cli._
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}
import com.madhouse.madmax.utils.Constant._
import com.madhouse.madmax.utils.ConfigReader._
import com.madhouse.madmax.utils.Functions._
import org.apache.spark.sql.streaming.Trigger

/**
  * Created by Madhouse on 2017/12/25.
  */
object MmReportRT {
  //val log: Logger = LoggerFactory.getLogger(ReportRT.getClass)

  def main(args: Array[String]): Unit = {

    var startOffsetEarliest = false
    var logType = WIN

    val opt = new Options()
    opt.addOption("e", "earliest", false, "set the start offset of kafka as earliest")
    opt.addOption("h", "help", false, "help message")
    opt.addOption("t", "type", true, "the type of log, such as req, imp, clk, win")

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
    if (cl.hasOption("t")) logType = cl.getOptionValue("t").toLowerCase

    log(s"#####logType = $logType, startOffsetEarliest = $startOffsetEarliest")

    val spark = SparkSession.builder.appName(s"MmReportRT_$logType").getOrCreate()
    import spark.implicits._

    val (offset, topic, maxOffsetPerTrigger, table, triggerTime, selectFields) =
      logType match {
        case REQ => (requestStartOffset, requestTopic, requestMaxOffsetPerTrigger,
          requestTable, requestTriggerTimeMS, REQSELECTFIELDS)
        case IMP => (impStartOffset, impTopic, impMaxOffsetPerTrigger,
          impTable, impTriggerTimeMS, TRASELECTFIELDS)
        case CLK => (clkStartOffset, clkTopic, clkMaxOffsetPerTrigger,
          clkTable, clkTriggerTimeMS, TRASELECTFIELDS)
        case WIN => (winStartOffset, winTopic, winMaxOffsetPerTrigger,
          winTable, winTriggerTimeMS, TRASELECTFIELDS)
      }

    val startingOffsets = if (startOffsetEarliest) EARLIEST else offset
    val ds = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("subscribe", topic)
      .option("startingOffsets", startingOffsets)
      .option("maxOffsetsPerTrigger", maxOffsetPerTrigger)
      .load()
      .select("value")
      .as[Array[Byte]]

    val recordDs = logType match {
      case REQ => ds.map(r => {
        val t = AvroUtils.decode(r, MediaBid.SCHEMA$).asInstanceOf[MediaBid]
        try {
          val (projectId, campaignId, creativeId) =
            if (t.getResponse != null) (t.getResponse.getProjectid.toInt,
              t.getResponse.getCid.toInt, t.getResponse.getCrid.toInt)
            else (0, 0, 0)
          RequestReport(timeprocess(t.getTs.toLong, 1800L), projectId, campaignId, creativeId,
            t.getRequest.getSspid.toInt, t.getRequest.getMediaid.toInt,
            t.getRequest.getAdspaceid.toInt, requesStatus(t.getStatus))
        } catch {
          case e: Throwable => log(s"#####get information from ${t.toString}, exception happened:$e")
            RequestReport(timeprocess(t.getTs.toLong, 1800L), 0, 0, 0, 0, 0, 0, requesStatus(0))
        }
      }).selectExpr(selectFields.split(","): _*).coalesce(8)
      case IMP => ds.map(r => {
        val t = AvroUtils.decode(r, ImpressionTrack.SCHEMA$).asInstanceOf[ImpressionTrack]
        TrackerReport(IMP, timeprocess(t.getTs.toLong, 1800L), t.getProjectid,
          t.getCid, t.getCrid, t.getSspid, t.getMediaid, t.getAdspaceid,
          t.getInvalidtype, t.getIncome, t.getCost)
      }).selectExpr(selectFields.split(","): _*).coalesce(8)
      case CLK => ds.map(r => {
        val t = AvroUtils.decode(r, ClickTrack.SCHEMA$).asInstanceOf[ClickTrack]
        TrackerReport(CLK, timeprocess(t.getTs.toLong, 1800L), t.getProjectid,
          t.getCid, t.getCrid, t.getSspid, t.getMediaid, t.getAdspaceid,
          t.getInvalidtype, t.getIncome, t.getCost)
      }).selectExpr(selectFields.split(","): _*).coalesce(8)
      case WIN => ds.map(r => {
        val t = AvroUtils.decode(r, WinNotice.SCHEMA$).asInstanceOf[WinNotice]
        TrackerReport(WIN, timeprocess(t.getTs.toLong, 1800L), t.getProjectid,
          t.getCid, t.getCrid, t.getSspid, t.getMediaid, t.getAdspaceid,
          0, 0, 0)
      }).selectExpr(selectFields.split(","): _*).coalesce(8)
    }

    val jdbcConf = JDBCConf(mysqlUrl, mysqlUser, mysqlPasswd, mysqlBatchSize, table)
    log(s"#####jdbcConf ===> ${jdbcConf.toString}")

    recordDs.writeStream
      .queryName(s"RTReport-Request-Query-$logType")
      .outputMode(MODE)
      .foreach(new JDBCSink(logType, jdbcConf))
      .option("checkpointLocation", s"/madmax/spark/checkpoint/$logType")
      .trigger(Trigger.ProcessingTime(triggerTime))
      .start()
      .awaitTermination()

  }
}
