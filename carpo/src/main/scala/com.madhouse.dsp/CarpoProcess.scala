package com.madhouse.dsp

import com.madhouse.dsp.entity._
import com.madhouse.dsp.utils.ConfigReader._
import com.madhouse.dsp.utils.Functions
import com.madhouse.dsp.utils.Functions._
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

/**
  * Created by Madhouse on 2018/1/12.
  */
object CarpoProcess extends Serializable {
  val log: Logger = LoggerFactory.getLogger(CarpoProcess.getClass)

  def dfDeal(rdf: DataFrame, tdf: DataFrame)(jcolumns: String)(scols: String): DataFrame = {
    rdf.join(tdf, jcolumns.split(","), "full_outer")
      .selectExpr((jcolumns + "," + scols).split(","): _*)
      .na.fill(0L, Seq(scols.split(","): _*))
  }

  def requestDeal(df: DataFrame, caseClassName: String)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val record =
      caseClassName match {
        case "RequestCampaignRecord" =>
          df.map(r => RequestCampaignRecord(r.getLong(0), r.getLong(1).toInt, r.getLong(2).toInt, r.getLong(3).toInt, r.getLong(4).toInt, r.getLong(5).toInt, r.getLong(7).toInt))
            .groupBy("timestamp", "project_id", "campaign_id", "material_id", "media_id", "adspace_id")
            .agg(sum("bids") as "bids", sum("wins") as "wins")
            .na.fill(0L, Seq("bids", "wins"))
        case "RequestCampaignLocationRecord" =>
          df.map(r => RequestCampaignLocationRecord(r.getLong(0), r.getLong(1).toInt, r.getLong(2).toInt, r.getLong(3).toInt, r.getString(6), r.getLong(7).toInt))
            .groupBy("timestamp", "project_id", "campaign_id", "material_id", "location")
            .agg(sum("bids") as "bids", sum("wins") as "wins")
            .na.fill(0L, Seq("bids", "wins"))
        case "RequestMediaRecord" =>
          df.map(r => RequestMediaRecord(r.getLong(0), r.getLong(4).toInt, r.getLong(5).toInt, r.getLong(7).toInt))
            .groupBy("timestamp", "media_id", "adspace_id")
            .agg(sum("reqs") as "reqs", sum("bids") as "bids", sum("wins") as "wins", sum("errs") as "errs")
            .na.fill(0L, Seq("reqs", "bids", "wins", "errs"))
        case "RequestMediaLocationRecord" =>
          df.map(r => RequestMediaLocationRecord(r.getLong(0), r.getLong(4).toInt, r.getLong(5).toInt, r.getString(6), r.getLong(7).toInt))
            .groupBy("timestamp", "media_id", "adspace_id", "location")
            .agg(sum("reqs") as "reqs", sum("bids") as "bids", sum("wins") as "wins", sum("errs") as "errs")
            .na.fill(0L, Seq("reqs", "bids", "wins", "errs"))
        case _ => null
      }
    record.toDF()
  }

  def trackDeal(idf: DataFrame, cdf: DataFrame, caseClassName: String)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val record =
      caseClassName match {
        case "TrackCampaignRecord" =>
          val i = idf.map(r => TrackCampaignRecord("imp", r.getLong(0), r.getLong(1).toInt, r.getLong(2).toInt, r.getLong(3).toInt, r.getLong(4).toInt, r.getLong(5).toInt, r.getLong(7).toInt, r.getLong(8), r.getLong(9)))
          val c = cdf.map(r => TrackCampaignRecord("clk", r.getLong(0), r.getLong(1).toInt, r.getLong(2).toInt, r.getLong(3).toInt, r.getLong(4).toInt, r.getLong(5).toInt, r.getLong(7).toInt, r.getLong(8), r.getLong(9)))
          i.union(c).groupBy("timestamp", "project_id", "campaign_id", "material_id", "media_id", "adspace_id")
            .agg(sum("imps") as "imps", sum("clks") as "clks", sum("vimps") as "vimps", sum("vclks") as "vclks", sum("cost") as "cost")
            .na.fill(0L, Seq("imps", "clks", "vimps", "vclks", "cost"))
        case "TrackCampaignLocationRecord" =>
          val i = idf.map(r => TrackCampaignLocationRecord("imp", r.getLong(0), r.getLong(1).toInt, r.getLong(2).toInt, r.getLong(3).toInt, r.getString(6), r.getLong(7).toInt, r.getLong(8), r.getLong(9)))
          val c = cdf.map(r => TrackCampaignLocationRecord("clk", r.getLong(0), r.getLong(1).toInt, r.getLong(2).toInt, r.getLong(3).toInt, r.getString(6), r.getLong(7).toInt, r.getLong(8), r.getLong(9)))
          i.union(c).groupBy("timestamp", "project_id", "campaign_id", "material_id", "location")
            .agg(sum("imps") as "imps", sum("clks") as "clks", sum("vimps") as "vimps", sum("vclks") as "vclks", sum("cost") as "cost")
            .na.fill(0L, Seq("imps", "clks", "vimps", "vclks", "cost"))
        case "TrackMediaRecord" =>
          val i = idf.map(r => TrackMediaRecord("imp", r.getLong(0), r.getLong(4).toInt, r.getLong(5).toInt, r.getLong(7).toInt, r.getLong(8), r.getLong(9)))
          val c = cdf.map(r => TrackMediaRecord("clk", r.getLong(0), r.getLong(4).toInt, r.getLong(5).toInt, r.getLong(7).toInt, r.getLong(8), r.getLong(9)))
          i.union(c).groupBy("timestamp", "media_id", "adspace_id")
            .agg(sum("imps") as "imps", sum("clks") as "clks", sum("vimps") as "vimps", sum("vclks") as "vclks", sum("income") as "income")
            .na.fill(0L, Seq("imps", "clks", "vimps", "vclks", "income"))
        case "TrackMediaLocationRecord" =>
          val i = idf.map(r => TrackMediaLocationRecord("imp", r.getLong(0), r.getLong(4).toInt, r.getLong(5).toInt, r.getString(6), r.getLong(7).toInt, r.getLong(8), r.getLong(9)))
          val c = cdf.map(r => TrackMediaLocationRecord("clk", r.getLong(0), r.getLong(4).toInt, r.getLong(5).toInt, r.getString(6), r.getLong(7).toInt, r.getLong(8), r.getLong(9)))
          i.union(c).groupBy("timestamp", "media_id", "adspace_id", "location")
            .agg(sum("imps") as "imps", sum("clks") as "clks", sum("vimps") as "vimps", sum("vclks") as "vclks", sum("income") as "income")
            .na.fill(0L, Seq("imps", "clks", "vimps", "vclks", "income"))
        case _ => null
      }
    record.toDF()
  }

  def process(spark: SparkSession, ts: Long, patch: Boolean = false): Unit = {
    import spark.implicits._
    println(s"#####start to process job with start time: $ts")
    spark.udf.register("TimeFunc", Functions.timeFunc)
    val partitionsPath = getPath(ts)
    println(s"#####partitionsPath = $partitionsPath")
    val isExists = isExistHdfsPath(partitionsPath)
    if (!(isExists(0) || isExists(1) || isExists(2))) {
      println(s"#####all(req,imp,clk) has no record logs in hdfs of half hour begins with $ts, exit....")
      //System.exit(0)
    } else {
      try {
        val reqDF = if (isExists(0)) {
          println(s"#####request has record logs in hdfs of half hour begins with $ts")
          val reqPath = mkString(hdfsBasePath, requestTopic, partitionsPath, "*")
          val reqParquet = spark.read.option("mergeSchema", "true").parquet(reqPath).coalesce(2)
          if (reqParquet.schema.fieldNames.contains("response")) {
            println(s"#####request has record schema fieldNames contains response")
            reqParquet.selectExpr("TimeFunc(time)", "coalesce(response.projectid,0L) as projectId", "coalesce(response.cid,0L) as cid", "coalesce(response.crid,0L) as crid", "request.mediaid", "request.adspaceid", "request.location", "status").cache
          }
          else reqParquet.selectExpr("TimeFunc(time)", "0L as projectId", "0L as cid", "0L as crid", "request.mediaid", "request.adspaceid", "request.location", "status").cache
        } else {
          println(s"#####request has no record logs in hdfs of half hour begins with $ts, using a null dataframe to replace")
          Seq(Request(0L, 0, 0, 0, 0, 0, 0)).toDF.filter("timestamp > 0")
        }
        println(s"#####request has ${reqDF.count()} records..")

        val impDF = if (isExists(1)) {
          println(s"#####imp has record logs in hdfs of half hour begins with $ts")
          val impPath = mkString(hdfsBasePath, impTopic, partitionsPath, "*")
          spark.read.parquet(impPath).coalesce(2).selectExpr("TimeFunc(time)", "projectid", "cid", "crid", "mediaid", "adspaceid", "location", "invalid", "income", "cost").cache
        } else {
          println(s"#####imp has no record logs in hdfs of half hour begins with $ts, using a null dataframe to replace")
          Seq(Track(0L, 0, 0, 0, 0, 0, 0, 0L, 0L)).toDF.filter("timestamp > 0")
        }
        println(s"#####impression has ${impDF.count()} records..")

        val clkDF = if (isExists(2)) {
          println(s"#####clk has record logs in hdfs of half hour begins with $ts")
          val clkPath = mkString(hdfsBasePath, clkTopic, partitionsPath, "*")
          spark.read.parquet(clkPath).coalesce(2).selectExpr("TimeFunc(time)", "projectid", "cid", "crid", "mediaid", "adspaceid", "location", "invalid", "income", "cost").cache
        } else {
          println(s"#####clk has no record logs in hdfs of half hour begins with $ts, using a null dataframe to replace")
          Seq(Track(0L, 0, 0, 0, 0, 0, 0, 0L, 0L)).toDF.filter("timestamp > 0")
        }
        println(s"#####click has ${clkDF.count()} records..")

        //campaign
        println(s"#####start to process campaign report...")
        val cTable = if (patch) campaignTablePathch else campaignTable
        val reqCampaignDF = requestDeal(reqDF, "RequestCampaignRecord")(spark)
        val traCampaignDF = trackDeal(impDF, clkDF, "TrackCampaignRecord")(spark)
        val joinCols = "timestamp,project_id,campaign_id,material_id"
        val selectCols = "bids,wins,imps,clks,vimps,vclks,cost"
        println(s"#####dataframe join by $joinCols, selected columns are $joinCols,$selectCols...")
        val campaignDF = dfDeal(reqCampaignDF, traCampaignDF)(joinCols)(selectCols).filter("project_id > 0").filter("bids>0 or imps>0 or clks>0 or cost>0").cache
        campaignDF.show
        println(s"#####campaignDF has ${campaignDF.count()} records, ready to save into table $cTable")
        campaignDF.coalesce(4).write.mode("append").jdbc(mysqlUrl, cTable, connectionProperties)
        campaignDF.unpersist()
        println(s"#####campaign report processed end...")

        //campaign with location
        println(s"#####start to process campaign report with location...")
        val cTableL = if (patch) campaignTableLocationPatch else campaignTableLocation
        val reqCampaignLocationDF = requestDeal(reqDF, "RequestCampaignLocationRecord")(spark)
        val traCampaignLocationDF = trackDeal(impDF, clkDF, "TrackCampaignLocationRecord")(spark)
        val joinCols1 = "timestamp,project_id,campaign_id,material_id,location"
        val selectCols1 = "bids,wins,imps,clks,vimps,vclks,cost"
        println(s"#####dataframe join by $joinCols1, selected columns are $joinCols1,$selectCols1...")
        val campaignLocationDF = dfDeal(reqCampaignLocationDF, traCampaignLocationDF)(joinCols1)(selectCols1).filter("project_id > 0").filter("bids>0 or imps>0 or clks>0 or cost>0").cache
        campaignLocationDF.show
        println(s"#####campaignLocationDF has ${campaignLocationDF.count()} records, ready to save into table $cTableL")
        campaignLocationDF.coalesce(4).write.mode("append").jdbc(mysqlUrl, cTableL, connectionProperties)
        campaignLocationDF.unpersist()
        println(s"#####campaign report with location processed end...")

        //media
        println(s"#####start to process media report...")
        val mTable = if (patch) mediaTablePatch else mediaTable
        val reqMediaDF = requestDeal(reqDF, "RequestMediaRecord")(spark)
        val traMediaDF = trackDeal(impDF, clkDF, "TrackMediaRecord")(spark)
        val joinCols2 = "timestamp,media_id,adspace_id"
        val selectCols2 = "reqs,bids,wins,errs,imps,clks,vimps,vclks,income"
        println(s"#####dataframe join by $joinCols2, selected columns are $joinCols2,$selectCols2...")
        val mediaDF = dfDeal(reqMediaDF, traMediaDF)(joinCols2)(selectCols2).filter("media_id > 0").filter("reqs>0 or imps>0 or clks>0 or income>0").cache
        mediaDF.show
        println(s"#####mediaDF has ${mediaDF.count()} records, ready to save into table $mTable")
        mediaDF.coalesce(4).write.mode("append").jdbc(mysqlUrl, mTable, connectionProperties)
        mediaDF.unpersist()
        println(s"#####media report processed end...")

        //media with location
        println(s"#####start to process media report with location...")
        val mTableL = if (patch) mediaTableLocationPatch else mediaTableLocation
        val reqMediaLocationDF = requestDeal(reqDF, "RequestMediaLocationRecord")(spark)
        val traMeidaLocationDF = trackDeal(impDF, clkDF, "TrackMediaLocationRecord")(spark)
        val joinCols3 = "timestamp,media_id,adspace_id,location"
        val selectCols3 = "reqs,bids,wins,errs,imps,clks,vimps,vclks,income"
        println(s"#####dataframe join by $joinCols3, selected columns are $joinCols3,$selectCols3...")
        val mediaLocationDF = dfDeal(reqMediaLocationDF, traMeidaLocationDF)(joinCols3)(selectCols3).filter("media_id > 0").filter("reqs>0 or imps>0 or clks>0 or income>0").cache
        mediaLocationDF.show
        println(s"#####mediaLocationDF has ${mediaLocationDF.count()} records, ready to save into table $mTableL")
        mediaLocationDF.coalesce(4).write.mode("append").jdbc(mysqlUrl, mTableL, connectionProperties)
        mediaLocationDF.unpersist()
        println(s"#####media report with location processed end...")

        reqDF.unpersist()
        impDF.unpersist()
        clkDF.unpersist()

      } catch {
        //case _: IOException => println(s"#####some of following paths are not exist!!\n#####reqPath=$reqPath\n#####impPath=$impPath\n#####clkPath=$clkPath")
        case e: Throwable => e.printStackTrace()
      }
    }

    println(s"#####process job with start time: $ts end...")
  }
}
