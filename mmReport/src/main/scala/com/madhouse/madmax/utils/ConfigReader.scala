package com.madhouse.madmax.utils

import java.io.{File, InputStreamReader}
import java.net.URI
import java.util.Properties

import com.madhouse.madmax.utils.Functions.log
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import com.madhouse.madmax.utils.Constant._

/**
  * Created by Madhouse on 2017/12/25.
  */
object ConfigReader extends Serializable {
  var config: Config = _

  var defaultHdfsPath = "/madmax/apps/mmReport"
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
  val hdfsBasePath: String = if (hdfsPath.endsWith("/")) hdfsPath.dropRight(1) else hdfsPath

  val mysqlUrl: String = getWithDefault("mysql.url", "jdbc:mysql://172.16.26.210:3306/madmax")
  val mysqlUser: String = getWithDefault("mysql.user", "root")
  val mysqlPasswd: String = getWithDefault("mysql.pwd", "123456")
  val connectionProperties = new Properties()
  connectionProperties.setProperty("user", mysqlUser)
  connectionProperties.setProperty("password", mysqlPasswd)
  connectionProperties.setProperty("driver", driver)

  val projectTable: String = getWithDefault("mysql.project_table", "mm_report_project")
  val campaignTable: String = getWithDefault("mysql.campaign_table", "mm_report_project_campaign")
  val campaignAudiencePackageTable: String = getWithDefault("mysql.campaign_audience_package_table", "mm_report_project_campaign_audience_package")
  val campaignAudienceTagTable: String = getWithDefault("mysql.campaign_audience_tag_table", "mm_report_project_campaign_audience_tag")
  val campaignConnTable: String = getWithDefault("mysql.campaign_conn_table", "mm_report_project_campaign_conn")
  val campaignDeviceTable: String = getWithDefault("mysql.campaign_device_table", "mm_report_project_campaign_device")
  val campaignLocationTable: String = getWithDefault("mysql.campaign_location_table", "mm_report_project_campaign_location")
  val campaignMaterialTable: String = getWithDefault("mysql.campaign_material_table", "mm_report_project_campaign_material")
  val campaignCarrierTable: String = getWithDefault("mysql.campaign_carrier_table", "mm_report_project_campaign_carrier")
  val sspTable: String = getWithDefault("mysql.ssp_table", "mm_report_project_ssp")
  val sspOnlyTable: String = getWithDefault("mysql.ssp_only_table", "mm_report_ssp")
  val sspMediaAdspaceTable: String = getWithDefault("mysql.ssp_media_adspace_table", "mm_report_ssp_media_adspace")


  val requestTopic: String = getWithDefault("request.topic_name", "madmax_req")
  val impTopic: String = getWithDefault("imp.topic_name", "madmax_imp")
  val clkTopic: String = getWithDefault("clk.topic_name", "madmax_clk")
  val winTopic: String = getWithDefault("win.topic_name", "madmax_win")
}
