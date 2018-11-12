package com.madhouse.madmax.utils

object Constant {
  val REQ = "req"
  val IMP = "imp"
  val CLK = "clk"
  val WIN = "win"

  val VALID = 1L
  val INVALID = 0L
  val NOCOSTINCOME = 0L

  val ZK = "zk"
  val MINUTE = 60
  val HALFHOUR = 1800
  val DAY = 86400

  val EARLIEST = "earliest"
  val LATEST = "latest"
  val MODE = "append"
  val FORMAT = "parquet"

  val driver = "com.mysql.jdbc.Driver"
  val REQUESTFIELDS = "timestamp, project_id, campaign_id, material_id, ssp_id, media_id, adspace_id, reqs, bids, errs, create_time"
  val TRACKERFIELDS = "timestamp, project_id, campaign_id, material_id, ssp_id, media_id, adspace_id, wins, imps, clks, vimps, vclks, income, cost, create_time"

  val REQSELECTFIELDS = "timestamp,projectId,campaignId,creativeId,mediaId,adSpaceId,reqs,bids,errs"
  val TRASELECTFIELDS = "timestamp,projectId,campaignId,creativeId,mediaId,adSpaceId,wins,imps,clks,vimps,vclks,income,cost"
}
