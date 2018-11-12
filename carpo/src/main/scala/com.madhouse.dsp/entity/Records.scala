package com.madhouse.dsp.entity

/**
  * Created by Madhouse on 2017/12/25.
  */
case class Request(timestamp: Long, projectId: Int, campaignId: Int, creativeId: Int, mediaId: Int, adSpaceId: Int, status: Int)
case class Track(timestamp: Long, projectId: Int, campaignId: Int, creativeId: Int, mediaId: Int,
                 adSpaceId: Int, invalid: Int, income: Long, cost: Long)

case class RequestCampaignRecord(timestamp: Long, project_id: Int, campaign_id: Int, material_id: Int, media_id: Int, adspace_id: Int, bids: Long, wins: Long)
case object RequestCampaignRecord {
  val VALID = 1L
  val INVALID = 0L

  def apply(timestamp: Long, projectId: Int, campaignId: Int, creativeId: Int, mediaId: Int, adspaceId: Int, status: Int): RequestCampaignRecord = {
    val value = status match {
      //req, bid, win, err
      case 200 => (VALID, VALID, VALID, INVALID)
      case 204 => (VALID, INVALID, INVALID, INVALID)
      case 400|500 => (VALID, INVALID, INVALID, VALID)
      case _ => (VALID, INVALID, INVALID, INVALID)
    }
    new RequestCampaignRecord(timestamp, projectId, campaignId, creativeId, mediaId, adspaceId, value._2, value._3)
  }
}

case class RequestCampaignLocationRecord(timestamp: Long, project_id: Int, campaign_id: Int, material_id: Int, location: String, bids: Long, wins: Long)
case object RequestCampaignLocationRecord {
  val VALID = 1L
  val INVALID = 0L

  def apply(timestamp: Long, projectId: Int, campaignId: Int, creativeId: Int, location: String, status: Int): RequestCampaignLocationRecord = {
    val value = status match {
      //req, bid, win, err
      case 200 => (VALID, VALID, VALID, INVALID)
      case 204 => (VALID, INVALID, INVALID, INVALID)
      case 400|500 => (VALID, INVALID, INVALID, VALID)
      case _ => (VALID, INVALID, INVALID, INVALID)
    }
    new RequestCampaignLocationRecord(timestamp, projectId, campaignId, creativeId, location, value._2, value._3)
  }
}

case class RequestMediaRecord(timestamp: Long, media_id: Int, adspace_id: Int, reqs: Long, bids: Long, wins: Long, errs: Long)
case object RequestMediaRecord {
  val VALID = 1L
  val INVALID = 0L

  def apply(timestamp: Long, mediaId: Int, adSpaceId: Int, status: Int): RequestMediaRecord = {
    val value = status match {
      //req, bid, win, err
      case 200 => (VALID, VALID, VALID, INVALID)
      case 204 => (VALID, INVALID, INVALID, INVALID)
      case 400|500 => (VALID, INVALID, INVALID, VALID)
      case _ => (VALID, INVALID, INVALID, INVALID)
    }
    new RequestMediaRecord(timestamp, mediaId, adSpaceId, value._1, value._2, value._3, value._4)
  }
}

case class RequestMediaLocationRecord(timestamp: Long, media_id: Int, adspace_id: Int, location: String, reqs: Long, bids: Long, wins: Long, errs: Long)
case object RequestMediaLocationRecord {
  val VALID = 1L
  val INVALID = 0L

  def apply(timestamp: Long, mediaId: Int, adSpaceId: Int, location: String, status: Int): RequestMediaLocationRecord = {
    val value = status match {
      //req, bid, win, err
      case 200 => (VALID, VALID, VALID, INVALID)
      case 204 => (VALID, INVALID, INVALID, INVALID)
      case 400|500 => (VALID, INVALID, INVALID, VALID)
      case _ => (VALID, INVALID, INVALID, INVALID)
    }
    new RequestMediaLocationRecord(timestamp, mediaId, adSpaceId, location, value._1, value._2, value._3, value._4)
  }
}

case class TrackCampaignRecord(timestamp: Long, project_id: Int, campaign_id: Int, material_id: Int, media_id: Int, adspace_id: Int,
                               imps: Long, clks: Long, vimps: Long, vclks: Long, cost: Long)
case object TrackCampaignRecord {
  val IMP = "imp"
  val CLK = "clk"
  val VALID = 1L
  val INVALID = 0L
  val NOCOSTINCOME = 0L

  def apply(logType: String, timestamp: Long, projectId: Int, campaignId: Int, creativeId: Int,
            mediaId: Int, adspaceId: Int, invalid: Int, income: Long, cost: Long): TrackCampaignRecord = {
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
    new TrackCampaignRecord(timestamp, projectId, campaignId, creativeId, mediaId, adspaceId,
      value._1, value._2, value._3, value._4, value._6)
  }
}

case class TrackCampaignLocationRecord(timestamp: Long, project_id: Int, campaign_id: Int, material_id: Int, location: String,
                                       imps: Long, clks: Long, vimps: Long, vclks: Long, cost: Long)
case object TrackCampaignLocationRecord {
  val IMP = "imp"
  val CLK = "clk"
  val VALID = 1L
  val INVALID = 0L
  val NOCOSTINCOME = 0L

  def apply(logType: String, timestamp: Long, projectId: Int, campaignId: Int, creativeId: Int,
            location: String, invalid: Int, income: Long, cost: Long): TrackCampaignLocationRecord = {
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
    new TrackCampaignLocationRecord(timestamp, projectId, campaignId, creativeId, location,
      value._1, value._2, value._3, value._4, value._6)
  }
}

case class TrackMediaRecord(timestamp: Long, media_id: Int, adspace_id: Int,
                            imps: Long, clks: Long, vimps: Long, vclks: Long, income: Long)
case object TrackMediaRecord {
  val IMP = "imp"
  val CLK = "clk"
  val VALID = 1L
  val INVALID = 0L
  val NOCOSTINCOME = 0L

  def apply(logType: String, timestamp: Long, mediaId: Int, adSpaceId: Int,
            invalid: Int, income: Long, cost: Long): TrackMediaRecord = {
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
    new TrackMediaRecord(timestamp, mediaId, adSpaceId,
      value._1, value._2, value._3, value._4, value._5)
  }
}

case class TrackMediaLocationRecord(timestamp: Long, media_id: Int, adspace_id: Int, location: String,
                                    imps: Long, clks: Long, vimps: Long, vclks: Long, income: Long)
case object TrackMediaLocationRecord {
  val IMP = "imp"
  val CLK = "clk"
  val VALID = 1L
  val INVALID = 0L
  val NOCOSTINCOME = 0L

  def apply(logType: String, timestamp: Long, mediaId: Int, adSpaceId: Int, location: String,
            invalid: Int, income: Long, cost: Long): TrackMediaLocationRecord = {
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
    new TrackMediaLocationRecord(timestamp, mediaId, adSpaceId, location,
      value._1, value._2, value._3, value._4, value._5)
  }
}



