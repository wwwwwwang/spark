package com.madhouse.dsp.utils

import com.madhouse.dsp.entity._
import org.apache.commons.logging.{Log, LogFactory}
import org.apache.spark.sql.{ForeachWriter, Row}

/**
  * Created by Madhouse on 2017/12/27.
  */
class JDBCSink(logType: String, conf: JDBCConf) extends ForeachWriter[Row] {
  val log: Log = LogFactory.getLog(getClass)
  val REQUEST = "request"
  val IMP = "imp"
  val CLK = "clk"
  val REQUESTFIELDS = "timestamp, project_id, campaign_id, material_id, media_id, adspace_id, reqs, bids, wins, errs, create_timestamp"
  val TRACKERFIELDS = "timestamp, project_id, campaign_id, material_id, media_id, adspace_id, imps, clks, vimps, vclks, income, cost, create_timestamp"


  val driver = "com.mysql.jdbc.Driver"
  var connection: java.sql.Connection = _
  var statement: java.sql.Statement = _
  var requestTargetMap: Map[String, RequestTarget] = Map[String, RequestTarget]()
  var trackTargetMap: Map[String, TrackTarget] = Map[String, TrackTarget]()

  def open(partitionId: Long, version: Long): Boolean = {
    Class.forName(driver)
    connection = java.sql.DriverManager.getConnection(conf.url, conf.user, conf.passwd)
    statement = connection.createStatement
    true
  }

  def process(value: Row): Unit = {
    rowToString(value)
  }

  def close(errorOrNull: Throwable): Unit = {
    insertToMysqlBatch()
    connection.close()
  }

  def rowToString(r: Row): Unit = {
    logType match {
      case REQUEST =>
        //timestamp: Long, projectId: Int, campaignId: Int, creativeId: Int, mediaId: Int, adSpaceId: Int,
        // reqs: Long, bids: Long, wins: Long, errs: Long
        val key = s"${r.getLong(0)},${r.getInt(1)},${r.getInt(2)},${r.getInt(3)},${r.getInt(4)},${r.getInt(5)}"
        val rawValue = requestTargetMap.getOrElse(key, RequestTarget(0L, 0L, 0L, 0L))
        val value = RequestTarget(rawValue.reqs + r.getLong(6), rawValue.bids + r.getLong(7),
          rawValue.wins + r.getLong(8), rawValue.errs + r.getLong(9))
        requestTargetMap += key -> value
      case IMP | CLK =>
        //timestamp: Long, projectId: Int, campaignId: Int, creativeId: Int, mediaId: Int, adSpaceId: Int,
        // imps: Long, clks: Long, vimps: Long, vclks: Long, income: Long, cost: Long
        val key = s"${r.getLong(0)},${r.getInt(1)},${r.getInt(2)},${r.getInt(3)},${r.getInt(4)},${r.getInt(5)}"
        val rawValue = trackTargetMap.getOrElse(key, TrackTarget(0L, 0L, 0L, 0L, 0L, 0L))
        val value = TrackTarget(rawValue.imps + r.getLong(6), rawValue.clks + r.getLong(7),
          rawValue.vimps + r.getLong(8), rawValue.vclks + r.getLong(9),
          rawValue.income + r.getLong(10), rawValue.cost + r.getLong(11))
        trackTargetMap += key -> value
    }
  }

  def insertToMysqlBatch(): Unit = {
    val now = System.currentTimeMillis()/1000
    logType match {
      case REQUEST =>
        val count = requestTargetMap.size
        log.info(s"#####there are $count records of $logType to be saved..")
        if (count > 0) {
          log.info(s"#####begin to save report records of $logType to mysql table: ${conf.table}")
          val fields = REQUESTFIELDS
          val sqlStrPrefix = s"insert into ${conf.table} ($fields) values "
          requestTargetMap.grouped(conf.batchSize).map(m => {
            m.map(r => s"(${r._1},${r._2.toString},$now)")
          }).foreach(r => {
            val value = r.mkString(",")
            val sqlStr = sqlStrPrefix + value
            log.info(s"#####sqlStr = $sqlStr")
            statement.execute(sqlStr)
            log.info(s"#####all records have been saved to mysql table....")
          })
        }
      case IMP | CLK =>
        val count = trackTargetMap.size
        log.info(s"#####there are $count records of $logType to be saved..")
        if (count > 0) {
          log.info(s"#####begin to save report records of $logType to mysql table: ${conf.table}")
          val fields = TRACKERFIELDS
          val sqlStrPrefix = s"insert into ${conf.table} ($fields) values "
          trackTargetMap.grouped(conf.batchSize).map(m => {
            m.map(r => s"(${r._1},${r._2.toString},$now)")
          }).foreach(r => {
            val value = r.mkString(",")
            val sqlStr = sqlStrPrefix + value
            log.info(s"#####sqlStr = $sqlStr")
            statement.execute(sqlStr)
            log.info(s"#####all records have been saved to mysql table....")
          })
        }
    }
  }
}