package com.madhouse.madmax

import org.apache.commons.cli._
import com.madhouse.madmax.utils.Functions._
import org.apache.spark.sql.SparkSession
import com.madhouse.madmax.utils.Constant._

/**
  * Created by Madhouse on 2017/12/25.
  */
object MmReport {
  def main(args: Array[String]): Unit = {
    var start = ""
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

    log(s"#####start = $start, end = $end")

    val startTime = System.currentTimeMillis()
    val jobs = dealStartAndEnd(start, end)
    log(s"there are ${jobs.size} jobs will be done, from: ${jobs.head}, to: ${jobs.last}, interval is half hour")

    val spark = SparkSession.builder().appName("MmReport").getOrCreate()

    for (job <- jobs) {
      ReportProcess.process(spark, job)
    }
    log(s"all jobs are finished, using time:${(System.currentTimeMillis() - startTime) / TOSECOND} s..")
  }
}
