package com.madhouse.madmax.utils

import java.io.{File, InputStreamReader}
import java.net.URI

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import com.madhouse.madmax.utils.Functions.log

/**
  * Created by Madhouse on 2017/12/25.
  */
object ConfigReader extends Serializable {
  var config: Config = _

  var defaultHdfsPath = "/madmax/apps/mmReportRT"
  var path: String = "application.conf"
  var rootName: String = "app"

  def init(configName: String, rootName: String): Unit = {
    log(s"#####config file's path = $defaultHdfsPath")
    val directory = new File("..")
    val filePath = directory.getAbsolutePath
    log(s"#####directory.getAbsolutePath = $filePath")
    val localPath = filePath.substring(filePath.indexOf(":") + 1, filePath.lastIndexOf("/") + 1) + configName
    log(s"#####path = $localPath")
    val configFile = new File(localPath)
    if (configFile.exists()) {
      config = ConfigFactory.parseFile(configFile).getConfig(rootName)
    } else {
      log(s"####Property file not found:$localPath, try to get it from hdfs...")

      val hdfsPath = defaultHdfsPath + "/" + configName
      log(s"#####start to read config($hdfsPath) file from hdfs")
      val conf: Configuration = new Configuration
      conf.setBoolean("fs.hdfs.impl.disable.cache", true)
      val fs = FileSystem.get(URI.create(hdfsPath), conf)
      if (fs.exists(new Path(hdfsPath))) {
        val in = fs.open(new Path(hdfsPath))
        /*val str = inputStream2String(in)
        log.info(s"#####string = $str")*/
        config = ConfigFactory.parseReader(new InputStreamReader(in)).getConfig(rootName)
        //config = ConfigFactory.parseString(inputStream2String(in)).getConfig(rootName)
        in.close()
        fs.close()
        log(s"#####config added from config file...")
      } else {
        log(s"####$hdfsPath in hdfs is not exist, cannot get config and exit...")
        fs.close()
        sys.exit(1)
      }
    }
  }

  def getWithElse[T](path: String, defaultValue: T): T = {
    if (config.hasPath(path)) {
      defaultValue match {
        case _: Int => config.getInt(path).asInstanceOf[T]
        case _: String => config.getString(path).asInstanceOf[T]
        case _: Double => config.getDouble(path).asInstanceOf[T]
        case _: Long => config.getLong(path).asInstanceOf[T]
        case _: Boolean => config.getBoolean(path).asInstanceOf[T]
        case _ => defaultValue
      }
    } else {
      defaultValue
    }
  }

  val configDefault: Unit = init(path, rootName)

  val bootstrapServers: String = getWithElse("kafka.bootstrap_servers", "")
  val mysqlUrl: String = getWithElse("mysql.url", "jdbc:mysql://172.16.26.210:3306/madmax")
  val mysqlUser: String = getWithElse("mysql.user", "root")
  val mysqlPasswd: String = getWithElse("mysql.pwd", "123456")
  val mysqlBatchSize: Int = getWithElse("mysql.batch_size", 100)

  val requestStartOffset: String = getWithElse("request.starting_offsets","latest")
  val requestTriggerTimeMS: Long = getWithElse("request.trigger_processing_time_ms", 3000L)
  val requestMaxOffsetPerTrigger: Int = getWithElse("request.max_offsets_per_trigger", 150000)
  val requestTopic :String = getWithElse("request.topic_name", "madmax_req")
  val requestTable: String = getWithElse("request.save_table", "mm_report_project_request_mem")

  val impStartOffset: String = getWithElse("imp.starting_offsets","latest")
  val impTriggerTimeMS: Long = getWithElse("imp.trigger_processing_time_ms", 3000L)
  val impMaxOffsetPerTrigger: Int = getWithElse("imp.max_offsets_per_trigger", 150000)
  val impTopic :String = getWithElse("imp.topic_name", "madmax_imp")
  val impTable: String = getWithElse("imp.save_table", "mm_report_project_tracker_mem")

  val clkStartOffset: String = getWithElse("clk.starting_offsets","latest")
  val clkTriggerTimeMS: Long = getWithElse("clk.trigger_processing_time_ms", 3000L)
  val clkMaxOffsetPerTrigger: Int = getWithElse("clk.max_offsets_per_trigger", 150000)
  val clkTopic :String = getWithElse("clk.topic_name", "madmax_clk")
  val clkTable: String = getWithElse("clk.save_table", "mm_report_project_tracker_mem")

  val winStartOffset: String = getWithElse("win.starting_offsets","latest")
  val winTriggerTimeMS: Long = getWithElse("win.trigger_processing_time_ms", 3000L)
  val winMaxOffsetPerTrigger: Int = getWithElse("win.max_offsets_per_trigger", 150000)
  val winTopic :String = getWithElse("win.topic_name", "madmax_win")
  val winTable: String = getWithElse("win.save_table", "mm_report_project_tracker_mem")
}
