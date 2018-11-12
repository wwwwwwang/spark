package com.madhouse.dsp.utils

import java.io.{File, InputStreamReader}
import java.net.URI

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.commons.logging.{Log, LogFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}

/**
  * Created by Madhouse on 2017/12/25.
  */
object ConfigReader extends Serializable {
  var config: Config = _
  val logConfig: Log = LogFactory.getLog(ConfigReader.getClass)

  var defaultHdfsPath = "/travelmad/apps/thallo"
  var path: String = "application.conf"
  var rootName: String = "app"

  def inputStream2String(is: FSDataInputStream): String = {
    scala.io.Source.fromInputStream(is).getLines().mkString("\n")
  }

  def init(configName: String, rootName: String): Unit = {
    //logConfig.info(s"#####config file's path = $defaultHdfsPath")
    val directory = new File("..")
    val filePath = directory.getAbsolutePath
    //logConfig.info(s"#####directory.getAbsolutePath = $filePath")
    val localPath = filePath.substring(filePath.indexOf(":") + 1, filePath.lastIndexOf("/") + 1) + configName
    //logConfig.info(s"#####path = $localPath")
    val configFile = new File(localPath)
    if (configFile.exists()) {
      config = ConfigFactory.parseFile(configFile).getConfig(rootName)
    } else {
      logConfig.info(s"####Property file not found:$localPath, try to get it from hdfs...")

      val hdfsPath = defaultHdfsPath + "/" + configName
      logConfig.info(s"#####start to read config($hdfsPath) file from hdfs")
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
        logConfig.info(s"#####config added from config file...")
      } else {
        logConfig.info(s"####$hdfsPath in hdfs is not exist, cannot get config and exit...")
        fs.close()
        sys.exit(1)
      }
    }
  }

  def getWithDefault[T](path: String, defaultValue: T): T = {
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

  val hdfsPath: String = getWithDefault("hdfs.base_path", "/travelmad/applogs/")
  val bootstrapServers: String = getWithDefault("kafka.bootstrap_servers", "")
  val zookeeperServers: String = getWithDefault("zookeeper.servers", "")
  val zookeeperOffsetBasePath: String = getWithDefault("zookeeper.offset_base_path", "/thallo")

  val hdfsBasePath: String = if (hdfsPath.endsWith("/")) hdfsPath.dropRight(1) else hdfsPath
  val zkBasePath: String = if (zookeeperOffsetBasePath.endsWith("/")) zookeeperOffsetBasePath.dropRight(1) else zookeeperOffsetBasePath

  val requestStartOffset: String = getWithDefault("request.starting_offsets", "latest")
  val requestStreamingInterval: Int = getWithDefault("request.interval", 60)
  val requestMaxRatePerPartition: Int = getWithDefault("request.kafka_max_rate_per_partition", 2000)
  val requestTopic: String = getWithDefault("request.topic_name", "test_tvl_request")

  val impStartOffset: String = getWithDefault("imp.starting_offsets", "latest")
  val impStreamingInterval: Int = getWithDefault("imp.interval", 60)
  val impMaxRatePerPartition: Int = getWithDefault("imp.kafka_max_rate_per_partition", 2000)
  val impTopic: String = getWithDefault("imp.topic_name", "test_tvl_imp")

  val clkStartOffset: String = getWithDefault("clk.starting_offsets", "latest")
  val clkStreamingInterval: Int = getWithDefault("clk.interval", 60)
  val clkMaxRatePerPartition: Int = getWithDefault("clk.kafka_max_rate_per_partition", 2000)
  val clkTopic: String = getWithDefault("clk.topic_name", "test_tvl_clk")
}
