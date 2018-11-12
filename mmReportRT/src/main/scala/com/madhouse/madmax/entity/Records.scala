package com.madhouse.madmax.entity

import com.madhouse.madmax.utils.Constant._

/**
  * Created by Madhouse on 2017/12/25.
  */
case class JDBCConf(url: String, user: String, passwd: String, batchSize: Int, table: String) {
  override def toString: String = {
    s"#####url=$url, user=$user, passwd=$passwd,batchSize=$batchSize, table=$table"
  }
}

case class RequestReport(timestamp: Long, projectId: Long, campaignId: Long, creativeId: Long,
                         sspid: Long, mediaId: Long, adSpaceId: Long, reqs: Long, bids: Long, errs: Long)

case class TrackerReport(timestamp: Long, projectId: Long, campaignId: Long, creativeId: Long,
                         sspid: Long, mediaId: Long, adSpaceId: Long, wins: Long, imps: Long,
                         clks: Long, vimps: Long, vclks: Long, income: Long, cost: Long)


case object RequestReport {
  def apply(timestamp: Long, projectId: Long, campaignId: Long, creativeId: Long,
            sspid: Long, mediaId: Long, adSpaceId: Long, status: Int): RequestReport = {
    val value = status match {
      //req, bid, err
      case 0 => (VALID, VALID, INVALID)
      case 1 => (VALID, INVALID, INVALID)
      case 2 => (VALID, INVALID, VALID)
    }
    RequestReport(timestamp, projectId, campaignId, creativeId,
      sspid, mediaId, adSpaceId, value._1, value._2, value._3)
  }
}

case object TrackerReport {
  def apply(logType: String, timestamp: Long, projectId: Long, campaignId: Long, creativeId: Long,
            sspid: Long, mediaId: Long, adSpaceId: Long, invalid: Int, income: Int, cost: Int): TrackerReport = {
    val value =
    //wins, imps , clks , vimps , vclks, income, cost
      if (logType.equalsIgnoreCase(IMP)) {
        if (invalid == INVALID)
          (INVALID, VALID, INVALID, VALID, INVALID, income.toLong, cost.toLong)
        else
          (INVALID, VALID, INVALID, INVALID, INVALID, NOCOSTINCOME, NOCOSTINCOME)
      } else if (logType.equalsIgnoreCase(CLK)) {
        if (invalid == INVALID)
          (INVALID, INVALID, VALID, INVALID, VALID, income.toLong * 1000, cost.toLong * 1000)
        else
          (INVALID, INVALID, VALID, INVALID, INVALID, NOCOSTINCOME, NOCOSTINCOME)
      } else {
        (VALID, INVALID, INVALID, INVALID, INVALID, NOCOSTINCOME, NOCOSTINCOME)
      }
    TrackerReport(timestamp, projectId, campaignId, creativeId,
      sspid, mediaId, adSpaceId, value._1, value._2, value._3,
      value._4, value._5, value._6, value._7)
  }
}

case class RequestTarget(reqs: Long, bids: Long, errs: Long) {
  override def toString: String = {
    s"$reqs,$bids,$errs"
  }
}

case class TrackerTarget(wins: Long, imps: Long, clks: Long, vimps: Long, vclks: Long, income: Long, cost: Long) {
  override def toString: String = {
    s"$wins,$imps,$clks,$vimps,$vclks,$income,$cost"
  }
}



