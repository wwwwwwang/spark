package com.madhouse.dsp

import org.apache.commons.cli._
import org.slf4j.{Logger, LoggerFactory}
import com.madhouse.dsp.utils.Functions._
import org.apache.spark.sql.SparkSession

/**
  * Created by Madhouse on 2017/12/25.
  */
object Carpo {
  val log: Logger = LoggerFactory.getLogger(Carpo.getClass)

  def main(args: Array[String]): Unit = {
    var start = ""
    var end = ""
    var patch = false

    val opt = new Options()

    opt.addOption("s", "start", true, "set the start time with format:yyyyMMdd or yyyyMMddHHmm")
    opt.addOption("h", "help", false, "help message")
    opt.addOption("e", "end", true, "set the start time with format:yyyyMMdd or yyyyMMddHHmm")
    opt.addOption("p", "patch", false, "whether use for patching data")

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
    if (cl.hasOption("p")) patch = true
    println(s"#####start = $start, end = $end, patch = $patch")

    val startTime = System.currentTimeMillis()
    val jobs = dealStartAndEnd(start, end)
    println(s"there are ${jobs.size} jobs will be done, from: ${jobs.head}, to: ${jobs.last}, interval is half hour")

    val spark = SparkSession.builder().appName("Carpo").getOrCreate()

    for (job <- jobs) {
      CarpoProcess.process(spark, job, patch)
    }
    println(s"all jobs are finished, using time:${(System.currentTimeMillis() - startTime) / 1000} s..")
  }
}
