package com.madhouse.dsp

import com.madhouse.dsp.avro.ClickTrack
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
object AuxoClick {
  val log: Logger = LoggerFactory.getLogger(AuxoClick.getClass)

  def main(args: Array[String]): Unit = {
    val CLK = "clk"
    var configPath = ""
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

    val spark = SparkSession.builder.appName("AuxoClick").getOrCreate()
    import spark.implicits._

    val startingOffsets = if (startOffsetEarliest) "earliest" else clkStartOffset
    val ds = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("subscribe", clkTopic)
      .option("startingOffsets", startingOffsets)
      .option("maxOffsetsPerTrigger", clkMaxOffsetPerTrigger)
      .load()
      .select("value")
      .as[Array[Byte]]

    val clkDs = ds.map(r => {
      val t = AvroUtils.decode(r, ClickTrack.SCHEMA$).asInstanceOf[ClickTrack]
      TrackerReport(CLK, timeprocess(t.getTime, 1800L), t.getProjectid.toInt,
        t.getCid.toInt, t.getCrid.toInt, t.getMediaid.toInt, t.getAdspaceid.toInt,
        t.getInvalid, t.getIncome.toLong, t.getCost.toLong)
    }).select("timestamp", "projectId", "campaignId", "creativeId", "mediaId", "adSpaceId", "imps", "clks", "vimps", "vclks", "income", "cost")
      .coalesce(8)

    val jdbcConf = JDBCConf(mysqlUrl, mysqlUser, mysqlPasswd, mysqlBatchSize, clkTable)

    if (outSinkConsole) {
      clkDs.writeStream
        .queryName(s"RTReport-Click-Query")
        .outputMode("append")
        .format("console")
        .trigger(Trigger.ProcessingTime(clkTriggerTimeMS))
        .start()
        .awaitTermination()
    } else {
      clkDs.writeStream
        .queryName(s"RTReport-Click-Query")
        .outputMode("append")
        .foreach(new JDBCSink(CLK, jdbcConf))
        .option("checkpointLocation", s"/travelmad/spark/checkpoint/$clkTopic")
        .trigger(Trigger.ProcessingTime(clkTriggerTimeMS))
        .start()
        .awaitTermination()
    }
  }
}
