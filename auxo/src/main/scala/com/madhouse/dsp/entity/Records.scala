package com.madhouse.dsp.entity

/**
  * Created by Madhouse on 2017/12/25.
  */
case class RequestReport(timestamp: Long, projectId: Int, campaignId: Int, creativeId: Int, mediaId: Int, adSpaceId: Int, reqs: Long, bids: Long, wins: Long, errs: Long)

case class TrackerReport(timestamp: Long, projectId: Int, campaignId: Int, creativeId: Int, mediaId: Int,
                         adSpaceId: Int, imps: Long, clks: Long, vimps: Long, vclks: Long, income: Long, cost: Long)

case class JDBCConf(url: String, user: String, passwd: String, batchSize: Int, table: String) {
  override def toString: String = {
    s"""
       |#####url=$url, user=$user, passwd=$passwd,
       |batchSize=$batchSize, table=$table
     """.stripMargin
  }
}

case class RequestTarget(reqs: Long, bids: Long, wins: Long, errs: Long) {
  override def toString: String = {
    s"$reqs,$bids,$wins,$errs"
  }
}

case class TrackTarget(imps: Long, clks: Long, vimps: Long, vclks: Long, income: Long, cost: Long) {
  override def toString: String = {
    s"$imps,$clks,$vimps,$vclks,$income,$cost"
  }
}

/*case class RequestTarget(var reqs: Long = 0L, var bids: Long = 0L, var wins: Long = 0L, var errs: Long = 0L) {
  override def toString: String = {
    s"$reqs,$bids,$wins,$errs"
  }
}

case class TrackTarget(var imps: Long = 0L, var clks: Long = 0L, var vimps: Long = 0L, var vclks: Long = 0L, var income: Long = 0L, var cost: Long = 0L) {
  override def toString: String = {
    s"$imps,$clks,$vimps,$vclks,$income,$cost"
  }
}*/

case object RequestReport {
  val VALID = 1L
  val INVALID = 0L

  def apply(timestamp: Long, projectId: Int, campaignId: Int, creativeId: Int, mediaId: Int, adSpaceId: Int, status: Int): RequestReport = {
    val value = status match {
      //req, bid, win, err
      case 0 => (VALID, VALID, VALID, INVALID)
      case 1 => (VALID, INVALID, INVALID, INVALID)
      case 2 => (VALID, INVALID, INVALID, VALID)
    }
    RequestReport(timestamp, projectId, campaignId, creativeId, mediaId, adSpaceId,
      value._1, value._2, value._3, value._4)
  }
}

case object TrackerReport {
  val IMP = "imp"
  val CLK = "clk"
  val VALID = 1L
  val INVALID = 0L
  val NOCOSTINCOME = 0L

  def apply(logType: String, timestamp: Long, projectId: Int, campaignId: Int, creativeId: Int, mediaId: Int,
            adSpaceId: Int, invalid: Int, income: Long, cost: Long): TrackerReport = {
    val value =
    //imps , clks , vimps , vclks, income, cost
      if (logType.equalsIgnoreCase(IMP)) {
        if (invalid == INVALID)
          (VALID, INVALID, VALID, INVALID, income, cost)
        else
          (VALID, INVALID, INVALID, INVALID, NOCOSTINCOME, NOCOSTINCOME)
      } else {
        if (invalid == INVALID)
          (INVALID, VALID, INVALID, VALID, income * 1000, cost * 1000)
        else
          (INVALID, VALID, INVALID, INVALID, NOCOSTINCOME, NOCOSTINCOME)
      }
    TrackerReport(timestamp, projectId, campaignId, creativeId, mediaId, adSpaceId,
      value._1, value._2, value._3, value._4, value._5, value._6)
  }
}



