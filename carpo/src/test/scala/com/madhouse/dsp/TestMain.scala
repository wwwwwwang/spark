package com.madhouse.dsp

import com.madhouse.dsp.utils.Functions.{dealStartAndEnd, getPath, mkString}
import org.apache.commons.cli._
import org.slf4j.{Logger, LoggerFactory}

/**
  * Created by Madhouse on 2018/1/16.
  */
object TestMain {
  val log: Logger = LoggerFactory.getLogger(TestMain.getClass)

  def main(args: Array[String]): Unit = {
    var start = "201801170930"
    var end = ""

    val opt = new Options()

    opt.addOption("s", "start", true, "set the start time with format:yyyyMMdd or yyyyMMddHHmm")
    opt.addOption("h", "help", false, "help message")
    opt.addOption("e", "end", true, "set the start time with format:yyyyMMdd or yyyyMMddHHmm")

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
    if (cl.hasOption("e")) end = cl.getOptionValue("e")
    if (cl.hasOption("h")) {
      formatter.printHelp(formatstr, opt)
      System.exit(0)
    }
    if (cl.hasOption("s")) start = cl.getOptionValue("s")
    println(s"#####start = $start, end = $end")
    val jobs = dealStartAndEnd(start, end)
    println(s"#####jobs'size = ${jobs.size}")
    val hdfsBasePath = "/travelmad/applogs"
    val requestTopic = "test_tvl_request"
    val impTopic = "test_tvl_imp"
    val clkTopic = "test_tvl_clk"
    for (job <- jobs) {
      val partitionsPath = getPath(job)
      val reqPath = mkString(hdfsBasePath, requestTopic, partitionsPath, "*")
      val impPath = mkString(hdfsBasePath, impTopic, partitionsPath, "*")
      val clkPath = mkString(hdfsBasePath, clkTopic, partitionsPath, "*")
      println(s"#####reqPath=$reqPath, impPath=$impPath, clkPath=$clkPath")
    }
  }
}
