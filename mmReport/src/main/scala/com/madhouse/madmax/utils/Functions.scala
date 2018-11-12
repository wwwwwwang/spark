package com.madhouse.madmax.utils

import java.net.URI
import java.sql.Timestamp
import java.time.format.DateTimeFormatter.ofPattern
import java.time.{Instant, LocalDateTime, ZoneId}

import com.madhouse.madmax.utils.ConfigReader._
import com.madhouse.madmax.utils.Constant._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by Madhouse on 2017/12/27.
  */
object Functions extends Serializable {
  def log(s: String): Unit = {
    println(s"[${timeStamp2Date()}] : $s")
  }

  def timeStamp2Date(zone: String = "GMT+8"): String = {
    val now = System.currentTimeMillis
    val ts = now - now % TOSECOND
    val triggerTime: LocalDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(ts), ZoneId.of(zone))
    triggerTime.toString
  }

  def timeProcess(time: Long, cell: Long): Long = {
    time.toString.length match {
      case 10 => time - time % cell
      case 13 => val sencond = time / TOSECOND
        sencond - sencond % cell
    }
  }

  def timeFunc: (Long) => Long = (time: Long) => {
    time.toString.length match {
      case 10 => time - time % HALFHOUR
      case 13 => val sencond = time / TOSECOND
        sencond - sencond % HALFHOUR
    }
  }

  val osvFunc: (String => String) = osv => {
    val version = {
      val v = osv.replaceAll("[a-zA-Z]*", "").replaceAll("_", ".")
      if (v.startsWith(".")) v.substring(1) else v
    }

    if (version.contains(".")) {
      val ps = version.split('.')
      if (ps.length >= 2) s"${ps(0)}.${ps(1)}.X" else s"${ps(0)}.0.X"
    } else "others"
  }

  val osFunc: (Long => Int) = {
    case os@(1 | 2 | 3) => os.toInt
    case _ => 3
  }

  val carrierFunc: (Long => Int) = car => {
    if (car < 0) 0 else car.toInt
  }

  def string2DateTime(s: String): LocalDateTime = {
    val now = LocalDateTime.now(ZoneId.of(ZONE))
    val dateTime = if (s.trim.equalsIgnoreCase("")) now else {
      s.length match {
        case 8 => LocalDateTime.parse(s + "0000", ofPattern(PATTERN))
        case 12 => LocalDateTime.parse(s, ofPattern(PATTERN))
      }
    }
    dateTime
  }

  def dateTime2Long(l: LocalDateTime): Long = {
    val t = Timestamp.from(l.atZone(ZoneId.of(ZONE)).toInstant)
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


  def mkPath(ts: Long): String = {
    val date = timeProcess(ts, DAY)
    s"$date/$ts"
  }

  def mkString(subStrs: String*): String = {
    subStrs.mkString("/")
  }

  def isExistHdfsPath(time: Long): ArrayBuffer[Boolean] = {
    val res: ArrayBuffer[Boolean] = ArrayBuffer[Boolean]()
    val path = hdfsBasePath
    val conf: Configuration = new Configuration
    conf.setBoolean("fs.hdfs.impl.disable.cache", true)
    val fs = FileSystem.get(URI.create(path), conf)
    val p = mkPath(time)
    res += fs.exists(new Path(mkString(hdfsBasePath, requestTopic, p)))
    res += fs.exists(new Path(mkString(hdfsBasePath, impTopic, p)))
    res += fs.exists(new Path(mkString(hdfsBasePath, clkTopic, p)))
    res += fs.exists(new Path(mkString(hdfsBasePath, winTopic, p)))
    res
  }

}
