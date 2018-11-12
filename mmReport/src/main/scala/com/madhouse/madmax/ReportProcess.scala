package com.madhouse.madmax

import com.madhouse.madmax.entity._
import com.madhouse.madmax.utils.Functions._
import com.madhouse.madmax.utils.ConfigReader._
import com.madhouse.madmax.utils.Constant._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.sum

import scala.collection.mutable.ArrayBuffer

/**
  * Created by Madhouse on 2018/1/12.
  */
object ReportProcess extends Serializable {

  //inner, outer, left_outer, right_outer, leftsemi
  def dfJoin(rdf: DataFrame, tdf: DataFrame, joinType: String = "full_outer")(jcolumns: String)
            (scols: String, selectJoinColumn: Boolean = true): DataFrame = {
    val finalSelect = if (selectJoinColumn) s"$jcolumns,$scols" else scols
    rdf.join(tdf, jcolumns.split(","), joinType)
      .selectExpr(finalSelect.split(","): _*)
      .na.fill(0L, Seq(scols.split(","): _*))
  }

  def dfGroup(df: DataFrame)(gcolumns: String)(scolumns: String): DataFrame = {
    val sumColumns = sumAndRename(scolumns)
    val groupColumns = gcolumns.split(",")
    df.groupBy(groupColumns.head, groupColumns.tail: _*).agg(sumColumns.head, sumColumns.tail: _*)
      .selectExpr((gcolumns + "," + scolumns).split(","): _*)
      .na.fill(0L, Seq(scolumns.split(","): _*))
  }

  def sumAndRename(column: String): Seq[Column] = {
    val res = ArrayBuffer[Column]()
    val cols = column.split(",")
    for (col <- cols) {
      val r = sum(col) as col
      res.append(r)
    }
    res
  }

  def processOneTable(tableType: String, reqDF: DataFrame, trackDF: DataFrame, groupByCols: String,
                      selectReqCols: String, selectTraCols: String, table: String, filterCol:String = ""): Unit = {
    log(s"#####start to process $tableType report...")
    val req = dfGroup(reqDF)(groupByCols)(selectReqCols)
    val tra = dfGroup(trackDF)(groupByCols)(selectTraCols)
    log(s"#####dataframe join by $groupByCols, selected columns are $groupByCols,$selectReqCols,$selectTraCols...")
    val resDF =
      if("".equalsIgnoreCase(filterCol))
        dfJoin(req, tra)(groupByCols)(s"$selectReqCols,$selectTraCols").cache
      else
        dfJoin(req, tra)(groupByCols)(s"$selectReqCols,$selectTraCols").filter(s"$filterCol > 0").cache
    resDF.show
    log(s"#####dataframe has ${resDF.count()} records, ready to save into table $table")
    resDF.coalesce(4).write.mode("append").jdbc(mysqlUrl, table, connectionProperties)
    resDF.unpersist()
    log(s"#####$tableType report processed end...")
  }


  def process(spark: SparkSession, ts: Long): Unit = {
    import spark.implicits._
    log(s"#####start to process job with start time: $ts")
    spark.udf.register("TimeFunc", timeFunc)
    spark.udf.register("OsvFunc", osvFunc)
    spark.udf.register("OsFunc", osFunc)
    spark.udf.register("CarrierFunc", carrierFunc)
    val dateTime = mkPath(ts)
    log(s"#####daytimestamp = $dateTime")
    val isExists = isExistHdfsPath(ts)
    if (!(isExists(0) || isExists(1) || isExists(2) || isExists(3))) {
      log(s"#####all(req,imp,clk,win) has no record logs in hdfs of half hour begins with $ts, exit....")
      //System.exit(0)
    } else {
      try {
        val reqDF = if (isExists(0)) {
          log(s"#####request has record logs in hdfs of half hour begins with $ts")
          val reqPath = mkString(hdfsBasePath, requestTopic, dateTime, "*")
          val reqParquet = spark.read.option("mergeSchema", "true").parquet(reqPath).coalesce(2)
          if (reqParquet.schema.fieldNames.contains("response")) {
            log(s"#####request has record schema fieldNames contains response")
            reqParquet.selectExpr("TimeFunc(ts)", "coalesce(response.projectid,0L) as projectId", "coalesce(response.cid,0L) as cid", "coalesce(response.crid,0L) as crid",
              "request.sspid", "request.mediaid", "request.adspaceid",
              "request.connectiontype", "OsFunc(request.os)", "OsvFunc(request.osv)", "request.location",
              "CarrierFunc(request.carrier)", "status")
              .map(r => Request(r.getLong(0), r.getLong(1), r.getLong(2), r.getLong(3), r.getLong(4), r.getLong(5), r.getLong(6),
                r.getLong(7), r.getInt(8), r.getString(9), r.getString(10), r.getInt(11), r.getLong(12)))
              .toDF().cache
          } else reqParquet.selectExpr("TimeFunc(ts)", "0L as projectId", "0L as cid", "0L as crid",
            "request.sspid", "request.mediaid", "request.adspaceid",
            "request.connectiontype", "OsFunc(request.os)", "OsvFunc(request.osv)", "request.location",
            "CarrierFunc(request.carrier)", "status")
            .map(r => Request(r.getLong(0), r.getLong(1), r.getLong(2), r.getLong(3), r.getLong(4), r.getLong(5), r.getLong(6),
              r.getLong(7), r.getInt(8), r.getString(9), r.getString(10), r.getInt(11), r.getLong(12)))
            .toDF().cache
        } else {
          log(s"#####request has no record logs in hdfs of half hour begins with $ts, using a null dataframe to replace")
          Seq(Request(0L, 0L, 0L, 0L, 0L, 0L, 0L, 0, 0, "", "", 0, 0)).toDF.filter("timestamp > 0")
        }
        log(s"#####request has ${reqDF.count()} records..")
        reqDF.show(10, truncate = false)

        val impDF = if (isExists(1)) {
          log(s"#####imp has record logs in hdfs of half hour begins with $ts")
          val impPath = mkString(hdfsBasePath, impTopic, dateTime, "*")
          spark.read.parquet(impPath).coalesce(2)
            .selectExpr("TimeFunc(ts)", "impid", "projectid", "cid", "crid", "sspid", "mediaid", "adspaceid",
              "connectiontype", "OsFunc(os)", "OsvFunc(osv)", "location", "CarrierFunc(carrier)", "invalidtype", "income", "cost")
            .map(r => Imp(r.getLong(0), r.getString(1), r.getLong(2), r.getLong(3), r.getLong(4), r.getLong(5), r.getLong(6), r.getLong(7),
              r.getLong(8), r.getInt(9), r.getString(10), r.getString(11), r.getInt(12), r.getLong(13), r.getLong(14), r.getLong(15)))
            .toDF()
        } else {
          log(s"#####imp has no record logs in hdfs of half hour begins with $ts, using a null dataframe to replace")
          Seq(Imp(0L, "", 0L, 0L, 0L, 0L, 0L, 0L, 0, 0, "", "", 0, 0, 0L, 0L)).toDF.filter("timestamp > 0")
        }
        //log(s"#####impression has ${impDF.count()} records..")

        val clkDF = if (isExists(2)) {
          log(s"#####clk has record logs in hdfs of half hour begins with $ts")
          val clkPath = mkString(hdfsBasePath, clkTopic, dateTime, "*")
          spark.read.parquet(clkPath).coalesce(2)
            .selectExpr("TimeFunc(ts)", "projectid", "cid", "crid", "sspid", "mediaid", "adspaceid",
              "connectiontype", "OsFunc(os)", "OsvFunc(osv)", "location", "CarrierFunc(carrier)", "invalidtype", "income", "cost")
            .map(r => Track(r.getLong(0), r.getLong(1), r.getLong(2), r.getLong(3), r.getLong(4), r.getLong(5), r.getLong(6),
              r.getLong(7), r.getInt(8), r.getString(9), r.getString(10), r.getInt(11), r.getLong(12), r.getLong(13), r.getLong(14)))
            .toDF()
        } else {
          log(s"#####imp has no record logs in hdfs of half hour begins with $ts, using a null dataframe to replace")
          Seq(Track(0L, 0L, 0L, 0L, 0L, 0L, 0L, 0, 0, "", "", 0, 0, 0L, 0L)).toDF.filter("timestamp > 0")
        }
        //log(s"#####click has ${clkDF.count()} records..")

        val winDF = if (isExists(3)) {
          log(s"#####win has record logs in hdfs of half hour begins with $ts")
          val winPath = mkString(hdfsBasePath, winTopic, dateTime, "*")
          spark.read.parquet(winPath).coalesce(2)
            .selectExpr("impid")
            .map(r => Win(r.getString(0))).toDF()
        } else {
          log(s"#####imp has no record logs in hdfs of half hour begins with $ts, using a null dataframe to replace")
          Seq(Win("")).toDF.filter('win > 0)
        }

        val i = dfJoin(impDF, winDF, "left_outer")(gcolsImpWin)(scolsImpWin, selectJoinColumn = false)
        val trackDF = i.union(clkDF).cache()
        log(s"#####track has ${trackDF.count()} records..")
        trackDF.show(10, truncate = false)

        //project
        processOneTable(PROJECT, reqDF, trackDF, gcolsPro, scolsReq, scolsTra, projectTable, PROJECT_ID)

        //campaign
        processOneTable(CAMPAIGN, reqDF, trackDF, gcolsCam, scolsReq, scolsTra, campaignTable, PROJECT_ID)

        //audience_package

        //audience_tag

        //conn
        processOneTable(CONN, reqDF, trackDF, gcolsConn, scolsReq, scolsTra, campaignConnTable, PROJECT_ID)

        //device
        processOneTable(DEVICE, reqDF, trackDF, gcolsDev, scolsReq, scolsTra, campaignDeviceTable, PROJECT_ID)

        //location
        processOneTable(LOCATION, reqDF, trackDF, gcolsLoc, scolsReq, scolsTra, campaignLocationTable, PROJECT_ID)

        //material
        processOneTable(MATERIAL, reqDF, trackDF, gcolsMat, scolsReq, scolsTra, campaignMaterialTable, PROJECT_ID)

        //carrier
        processOneTable(CARRIER, reqDF, trackDF, gcolsCar, scolsReq, scolsTra, campaignCarrierTable, PROJECT_ID)

        //ssp
        //processOneTable(SSP, reqDF, trackDF, gcolsSsp, scolsReq1, scolsTra1, sspTable, PROJECT_ID)

        //ssp only
        processOneTable(SSP_ONLY, reqDF, trackDF, gcolsSspOnly, scolsReq1, scolsTra1, sspOnlyTable, SSP_ID)

        //ssp_media_adspace
        processOneTable(SSP_MEDIA_ADSAPCE, reqDF, trackDF, gcolsSspMediaAds, scolsReq1, scolsTra1, sspMediaAdspaceTable, SSP_ID)

        reqDF.unpersist()
        trackDF.unpersist()
      } catch {
        //case _: IOException => log(s"#####some of following paths are not exist!!\n#####reqPath=$reqPath\n#####impPath=$impPath\n#####clkPath=$clkPath")
        case e: Throwable => e.printStackTrace()
      }
    }

    log(s"#####process job with start time: $ts end...")
  }
}
