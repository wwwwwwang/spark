package com.madhouse.madmax.utils

import java.time.{Instant, LocalDateTime, ZoneId}

/**
  * Created by Madhouse on 2017/12/27.
  */
object Functions extends Serializable {
  def log(s: String): Unit = {
    println(s"[${timeStamp2Date()}] : $s")
  }

  def timeStamp2Date(zone: String = "GMT+8"): String = {
    val now = System.currentTimeMillis
    val ts = now - now % 1000
    val triggerTime: LocalDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(ts), ZoneId.of(zone))
    triggerTime.toString
  }

  def timeprocess(time: Long, cell: Long): Long = {
    time.toString.length match {
      case 10 => time - time % cell
      case 13 => val sencond = time / 1000
        sencond - sencond % cell
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
}
