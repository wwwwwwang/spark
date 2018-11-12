package com.madhouse.madmax.utils

import java.io.{File, InputStreamReader}
import java.net.URI

import com.madhouse.madmax.utils.Functions.log
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}

/**
  * Created by Madhouse on 2017/12/25.
  */
object ConfigReader extends Serializable {
  var config: Config = _
  var defaultHdfsPath = "/madmax/apps/mmLogCollector"
  var path: String = "application.conf"
  var rootName: String = "app"

  def init(configName: String, rootName: String): Unit = {
    //logConfig.info(s"#####config file's path = $defaultHdfsPath")
    val directory = new File("..")
    val filePath = directory.getAbsolutePath
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
        config = ConfigFactory.parseReader(new InputStreamReader(in)).getConfig(rootName)
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

  val hdfsPath: String = getWithDefault("hdfs.base_path", "/madmax/applogs/")
  val bootstrapServers: String = getWithDefault("kafka.bootstrap_servers", "")
  val zookeeperServers: String = getWithDefault("zookeeper.servers", "")
  val zookeeperOffsetBasePath: String = getWithDefault("zookeeper.offset_base_path", "/madmax_log_collector")

  val hdfsBasePath: String = if (hdfsPath.endsWith("/")) hdfsPath.dropRight(1) else hdfsPath
  val zkBasePath: String = if (zookeeperOffsetBasePath.endsWith("/")) zookeeperOffsetBasePath.dropRight(1) else zookeeperOffsetBasePath

  val reqStartOffset: String = getWithDefault("request.starting_offsets", "latest")
  val reqStreamingInterval: Int = getWithDefault("request.interval", 60)
  val reqMaxRatePerPartition: Int = getWithDefault("request.kafka_max_rate_per_partition", 2000)
  val reqTopic: String = getWithDefault("request.topic_name", "madmax_req")
  val reqPartitions: Int = getWithDefault("request.partition_numbers", 8)

  val impStartOffset: String = getWithDefault("imp.starting_offsets", "latest")
  val impStreamingInterval: Int = getWithDefault("imp.interval", 60)
  val impMaxRatePerPartition: Int = getWithDefault("imp.kafka_max_rate_per_partition", 2000)
  val impTopic: String = getWithDefault("imp.topic_name", "madmax_imp")
  val impPartitions: Int = getWithDefault("imp.partition_numbers", 4)

  val clkStartOffset: String = getWithDefault("clk.starting_offsets", "latest")
  val clkStreamingInterval: Int = getWithDefault("clk.interval", 60)
  val clkMaxRatePerPartition: Int = getWithDefault("clk.kafka_max_rate_per_partition", 2000)
  val clkTopic: String = getWithDefault("clk.topic_name", "madmax_clk")
  val clkPartitions: Int = getWithDefault("clk.partition_numbers", 4)

  val winStartOffset: String = getWithDefault("win.starting_offsets", "latest")
  val winStreamingInterval: Int = getWithDefault("win.interval", 60)
  val winMaxRatePerPartition: Int = getWithDefault("win.kafka_max_rate_per_partition", 2000)
  val winTopic: String = getWithDefault("win.topic_name", "madmax_win")
  val winPartitions: Int = getWithDefault("win.partition_numbers", 4)
}
