package com.madhouse.madmax.entity

import com.madhouse.madmax.utils.Constant._

/**
  * Created by Madhouse on 2017/12/25.
  */

case class Request(timestamp: Long, project_id: Long, campaign_id: Long, material_id: Long,
                   ssp_id: Long, media_id: Long, adspace_id: Long,
                   conn: Long, os: Int, osv: String, location: String, carrier: Int,
                   //audience
                   reqs: Long, bids: Long, errs: Long)

case object Request {
  def apply(timestamp: Long, projectId: Long, campaignId: Long, creativeId: Long,
            sspId: Long, mediaId: Long, adspaceId: Long,
            conn: Long, os: Int, osv: String, location: String, carrier: Int,
            //audience
            status: Long): Request = {
    val value = status match {
      //req, bid, err
      case 200 => (VALID, VALID, INVALID)
      case 204 => (VALID, INVALID, INVALID)
      case 400 | 500 => (VALID, INVALID, VALID)
      case _ => (VALID, INVALID, INVALID)
    }
    new Request(timestamp, projectId, campaignId, creativeId,
      sspId, mediaId, adspaceId, conn, os, osv, location, carrier,
      value._1, value._2, value._3)
  }
}

case class Imp(timestamp: Long, imp_id: String,
               project_id: Long, campaign_id: Long, material_id: Long,
               ssp_id: Long, media_id: Long, adspace_id: Long,
               conn: Long, os: Int, osv: String, location: String, carrier: Int,
               //audience
               imps: Long, clks: Long, vimps: Long, vclks: Long, income: Long, cost: Long)

case object Imp {
  def apply(timestamp: Long, impId: String,
            projectId: Long, campaignId: Long, creativeId: Long,
            sspId: Long, mediaId: Long, adspaceId: Long,
            conn: Long, os: Int, osv: String, location: String, carrier: Int,
            //audience
            invalid: Long, income: Long, cost: Long): Imp = {
    val value =
    //imps , clks , vimps , vclks, income, cost
      if (invalid == INVALID)
        (VALID, INVALID, VALID, INVALID, income, cost)
      else
        (VALID, INVALID, INVALID, INVALID, NOCOSTINCOME, NOCOSTINCOME)
    new Imp(timestamp, impId, projectId, campaignId, creativeId,
      sspId, mediaId, adspaceId, conn, os, osv, location, carrier,
      value._1, value._2, value._3, value._4, value._5, value._6)
  }
}

case class Track(timestamp: Long,
                 project_id: Long, campaign_id: Long, material_id: Long,
                 ssp_id: Long, media_id: Long, adspace_id: Long,
                 conn: Long, os: Int, osv: String, location: String, carrier: Int,
                 //audience
                 wins: Long, imps: Long, clks: Long, vimps: Long, vclks: Long, income: Long, cost: Long)

case object Track {
  def apply(timestamp: Long,
            projectId: Long, campaignId: Long, creativeId: Long,
            sspId: Long, mediaId: Long, adspaceId: Long,
            conn: Long, os: Int, osv: String, location: String, carrier: Int,
            //audience
            invalid: Long, income: Long, cost: Long): Track = {
    val value =
    //imps , clks , vimps , vclks, income, cost
      if (invalid == INVALID)
        (INVALID, VALID, INVALID, VALID, income * 1000, cost * 1000)
      else
        (INVALID, VALID, INVALID, INVALID, NOCOSTINCOME, NOCOSTINCOME)

    new Track(timestamp, projectId, campaignId, creativeId,
      sspId, mediaId, adspaceId, conn, os, osv, location, carrier, 0,
      value._1, value._2, value._3, value._4, value._5, value._6)
  }
}

case class Win(imp_id: String, wins:Long)
case object Win {
  def apply(imp_id: String): Win = {
    new Win(imp_id, VALID)
  }
}
