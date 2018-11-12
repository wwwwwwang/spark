package com.madhouse.madmax.utils

import com.madhouse.madmax.entity._
import com.madhouse.madmax.utils.Constant._
import com.madhouse.madmax.utils.Functions.log
import org.apache.spark.sql.{ForeachWriter, Row}

/**
  * Created by Madhouse on 2017/12/27.
  */
class JDBCSink(logType: String, conf: JDBCConf) extends ForeachWriter[Row] {
  var connection: java.sql.Connection = _
  var statement: java.sql.Statement = _
  var requestTargetMap: Map[String, RequestTarget] = Map[String, RequestTarget]()
  var trackTargetMap: Map[String, TrackerTarget] = Map[String, TrackerTarget]()

  def open(partitionId: Long, version: Long): Boolean = {
    Class.forName(driver)
    connection = java.sql.DriverManager.getConnection(conf.url, conf.user, conf.passwd)
    statement = connection.createStatement
    true
  }

  def process(r: Row): Unit = {
    rowToMap(r)
  }

  def close(errorOrNull: Throwable): Unit = {
    jdbcSave(logType)
    connection.close()
  }

  def rowToMap(r: Row): Unit = {
    logType match {
      case REQ =>
        //timestamp: Long, projectId: Long, campaignId: Long, creativeId: Long, sspId: Long, mediaId: Long, adSpaceId: Long,
        // reqs: Long, bids: Long, errs: Long
        val key = s"${r.getLong(0)},${r.getLong(1)},${r.getLong(2)},${r.getLong(3)},${r.getLong(4)},${r.getLong(5)},${r.getLong(6)}"
        val rawValue = requestTargetMap.getOrElse(key, RequestTarget(0L, 0L, 0L))
        val value = RequestTarget(rawValue.reqs + r.getLong(6),
          rawValue.bids + r.getLong(7), rawValue.errs + r.getLong(8))
        requestTargetMap += key -> value
      case IMP | CLK | WIN =>
        //timestamp: Long, projectId: Long, campaignId: Long, creativeId: Long, sspId: Long, mediaId: Long, adSpaceId: Long,
        // wins: Long, imps: Long, clks: Long, vimps: Long, vclks: Long, income: Long, cost: Long
        val key = s"${r.getLong(0)},${r.getLong(1)},${r.getLong(2)},${r.getLong(3)},${r.getLong(4)},${r.getLong(5)},${r.getLong(6)}"
        val rawValue = trackTargetMap.getOrElse(key, TrackerTarget(0L, 0L, 0L, 0L, 0L, 0L, 0L))
        val value = TrackerTarget(rawValue.wins + r.getLong(6), rawValue.imps + r.getLong(7),
          rawValue.clks + r.getLong(8), rawValue.vimps + r.getLong(9), rawValue.vclks + r.getLong(10),
          rawValue.income + r.getLong(11), rawValue.cost + r.getLong(12))
        trackTargetMap += key -> value
    }
  }

  def jdbcSave(t: String): Unit = {
    val (map, fields) = if (t.equalsIgnoreCase(REQ))
      (requestTargetMap, REQUESTFIELDS) else (trackTargetMap, TRACKERFIELDS)
    //val now = System.currentTimeMillis() / 1000
    val count = map.size
    log(s"#####there are $count records of $t to be saved..")
    if (count > 0) {
      log(s"#####begin to save report records of $t to mysql table: ${conf.table}")
      val sqlStrPrefix = s"insert into ${conf.table} ($fields) values "
      map.grouped(conf.batchSize).map(m => {
        //m.map(r => s"(${r._1},${r._2.toString},$now)")
        m.map(r => s"(${r._1},${r._2.toString})")
      }).foreach(r => {
        val value = r.mkString(",")
        val sqlStr = sqlStrPrefix + value
        log(s"#####sqlStr = $sqlStr")
        statement.execute(sqlStr)
        log(s"#####all records have been saved to mysql table....")
      })
    }
  }

  /*def insertToMysqlBatch(): Unit = {
    val now = System.currentTimeMillis() / 1000
    logType match {
      case REQ =>
        val count = requestTargetMap.size
        log(s"#####there are $count records of $logType to be saved..")
        if (count > 0) {
          log(s"#####begin to save report records of $logType to mysql table: ${conf.table}")
          val fields = REQUESTFIELDS
          val sqlStrPrefix = s"insert into ${conf.table} ($fields) values "
          requestTargetMap.grouped(conf.batchSize).map(m => {
            m.map(r => s"(${r._1},${r._2.toString},$now)")
          }).foreach(r => {
            val value = r.mkString(",")
            val sqlStr = sqlStrPrefix + value
            log(s"#####sqlStr = $sqlStr")
            statement.execute(sqlStr)
            log(s"#####all records have been saved to mysql table....")
          })
        }
      case IMP | CLK =>
        val count = trackTargetMap.size
        log(s"#####there are $count records of $logType to be saved..")
        if (count > 0) {
          log(s"#####begin to save report records of $logType to mysql table: ${conf.table}")
          val fields = TRACKERFIELDS
          val sqlStrPrefix = s"insert into ${conf.table} ($fields) values "
          trackTargetMap.grouped(conf.batchSize).map(m => {
            m.map(r => s"(${r._1},${r._2.toString},$now)")
          }).foreach(r => {
            val value = r.mkString(",")
            val sqlStr = sqlStrPrefix + value
            log(s"#####sqlStr = $sqlStr")
            statement.execute(sqlStr)
            log(s"#####all records have been saved to mysql table....")
          })
        }
    }
  }*/
}