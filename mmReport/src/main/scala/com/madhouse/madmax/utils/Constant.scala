package com.madhouse.madmax.utils

object Constant {
  val REQ = "req"
  val IMP = "imp"
  val CLK = "clk"
  val WIN = "win"

  val VALID = 1L
  val INVALID = 0L
  val NOCOSTINCOME = 0L

  val TOSECOND = 1000
  val MINUTE = 60
  val HALFHOUR = 1800
  val DAY = 86400
  val PATTERN = "yyyyMMddHHmm"
  val ZONE = "Asia/Shanghai"

  val driver = "com.mysql.jdbc.Driver"

  val gcolsImpWin = "imp_id"
  val scolsImpWin = "timestamp,project_id,campaign_id,material_id,ssp_id,media_id,adspace_id,conn,os,osv,location,carrier,wins,imps,clks,vimps,vclks,income,cost"

  val scolsReq = "bids"
  val scolsTra = "wins,imps,clks,vimps,vclks,cost"
  val scolsReq1 = "reqs,bids,errs"
  val scolsTra1 = "imps,clks,vimps,vclks,income"

  val PROJECT = "project"
  val gcolsPro = "timestamp,project_id"
  val CAMPAIGN = "campaign"
  val gcolsCam = "timestamp,project_id,campaign_id"
  val CONN = "campaign_conn"
  val gcolsConn = "timestamp,project_id,campaign_id,conn"
  val DEVICE = "campaign_device"
  val gcolsDev = "timestamp,project_id,campaign_id,os,osv"
  val LOCATION = "campaign_location"
  val gcolsLoc = "timestamp,project_id,campaign_id,location"
  val MATERIAL = "campaign_material"
  val gcolsMat = "timestamp,project_id,campaign_id,material_id"
  val CARRIER = "campaign_carrier"
  val gcolsCar = "timestamp,project_id,campaign_id,carrier"
  val SSP = "ssp"
  val gcolsSsp = "timestamp,project_id,ssp_id,media_id,adspace_id"
  val SSP_ONLY = "ssp"
  val gcolsSspOnly = "timestamp,ssp_id"
  val SSP_MEDIA_ADSAPCE = "ssp_media_adspace"
  val gcolsSspMediaAds = "timestamp,ssp_id,media_id,adspace_id"

  val PROJECT_ID = "project_id"
  val SSP_ID = "ssp_id"
}
