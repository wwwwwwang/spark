package com.madhouse.dsp.utils

import java.net.URI
import java.sql.Timestamp
import java.time.format.DateTimeFormatter.ofPattern
import java.time.{LocalDateTime, ZoneId}

import com.madhouse.dsp.utils.ConfigReader.{clkTopic, hdfsBasePath, impTopic, requestTopic}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by Madhouse on 2017/12/27.
  */
object Functions extends Serializable {
  val ZONE: ZoneId = ZoneId.of("Asia/Shanghai")
  val PATTERN = "yyyyMMddHHmm"
  val HALFHOUR = 1800
  val MINUTE = 60

  def timeProcess(time: Long, cell: Long): Long = {
    time.toString.length match {
      case 10 => time - time % cell
      case 13 => val sencond = time / 1000
        sencond - sencond % cell
    }
  }

  def timeFunc: (Long) => Long = (time: Long) => {
    time.toString.length match {
      case 10 => time - time % 1800
      case 13 => val sencond = time / 1000
        sencond - sencond % 1800
    }
  }

  def requesStatus(status: Int): Int = {
    status match {
      case 200 => 0
      case 204 => 1
      case 400 | 500 => 2
      case _ => 1
    }
  }

  def string2DateTime(s: String): LocalDateTime = {
    val now = LocalDateTime.now(ZONE)
    val dateTime = if (s.trim.equalsIgnoreCase("")) now else {
      s.length match {
        case 8 => LocalDateTime.parse(s + "0000", ofPattern(PATTERN))
        case 12 => LocalDateTime.parse(s, ofPattern(PATTERN))
      }
    }
    dateTime
  }

  def dateTime2Long(l: LocalDateTime): Long = {
    val t = Timestamp.from(l.atZone(ZONE).toInstant)
    timeProcess(t.getTime, HALFHOUR)
  }

  def dealStartAndEnd(s: String, e: String): ArrayBuffer[Long] = {
    val timestamps: ArrayBuffer[Long] = ArrayBuffer[Long]()
    val start = dateTime2Long(string2DateTime(s))
    val end = if (e.trim.equalsIgnoreCase("")) start else dateTime2Long(string2DateTime(e))
    val cnt = (end - start - MINUTE) / HALFHOUR
    for (i <- 0 to cnt.toInt) {
      timestamps += start + HALFHOUR * i
    }
    timestamps
  }

  def getPath(ts: Long): String = {
    val paths: ArrayBuffer[Long] = ArrayBuffer[Long]()
    for (i <- 0 until HALFHOUR / MINUTE) {
      paths += ts + MINUTE * i
    }
    paths.mkString("{", ",", "}")
  }

  def mkString(subStrs: String*): String = {
    subStrs.mkString("/")
  }

  def isExistHdfsPath(s:String): ArrayBuffer[Boolean] ={
    val res: ArrayBuffer[Boolean] = ArrayBuffer[Boolean]()
    val path = "/travelmad/applogs"
    val conf: Configuration = new Configuration
    conf.setBoolean("fs.hdfs.impl.disable.cache", true)
    val fs = FileSystem.get(URI.create(path), conf)
    val times = s.replaceAll("\\{","").replaceAll("\\}","").split(",")
    var req = false
    var imp = false
    var clk = false
    for(time <- times){
      if (fs.exists(new Path(mkString(hdfsBasePath, requestTopic,time)))){
        req = true
      }
      if (fs.exists(new Path(mkString(hdfsBasePath, impTopic,time)))){
        imp = true
      }
      if (fs.exists(new Path(mkString(hdfsBasePath, clkTopic,time)))){
        clk = true
      }
    }
    res += req
    res += imp
    res += clk
    res
  }

}
