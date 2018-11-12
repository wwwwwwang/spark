package com.madhouse.madmax.utils

object Constant {
  val REQ = "req"
  val IMP = "imp"
  val CLK = "clk"
  val WIN = "win"

  val VALID = 1L
  val INVALID = 0L
  val NOCOSTINCOME = 0L

  val EARLIEST = "earliest"
  val MODE = "append"

  val driver = "com.mysql.jdbc.Driver"
  val REQUESTFIELDS = "timestamp, project_id, campaign_id, material_id, ssp_id, media_id, adspace_id, reqs, bids, errs"
  val TRACKERFIELDS = "timestamp, project_id, campaign_id, material_id, ssp_id, media_id, adspace_id, wins, imps, clks, vimps, vclks, income, cost"

  val REQSELECTFIELDS = "timestamp,projectId,campaignId,creativeId,mediaId,adSpaceId,reqs,bids,errs"
  val TRASELECTFIELDS = "timestamp,projectId,campaignId,creativeId,mediaId,adSpaceId,wins,imps,clks,vimps,vclks,income,cost"
}
