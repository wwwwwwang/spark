package com.madhouse.dsp.utils

import com.google.common.base.Charsets
import com.madhouse.dsp.utils.ConfigReader._
import kafka.api.{OffsetRequest, PartitionOffsetRequestInfo, TopicMetadataRequest}
import kafka.common.TopicAndPartition
import kafka.consumer.SimpleConsumer
import kafka.utils.ZkUtils
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.serialize.ZkSerializer
import org.apache.spark.streaming.kafka.OffsetRange
import org.slf4j.{Logger, LoggerFactory}

/**
  * Created by Madhouse on 2018/1/10.
  */
object OffsetUtils extends Serializable {
  val log: Logger = LoggerFactory.getLogger(OffsetUtils.getClass)
  var zkClient: ZkClient = _
  var offsetPath = ""
  var topic = ""

  def init(logType: String): Unit = {
    zkClient = new ZkClient(zookeeperServers, 50000, 50000)
    zkClient.setZkSerializer(MyZkSerializer)
    val t = logType.toLowerCase
    topic = if (t.contains("req")) {
      requestTopic
    } else if (t.contains("imp")) {
      impTopic
    } else {
      clkTopic
    }
    offsetPath = zkBasePath + "/" + topic
    log.info(s"##### init finished, offset path of $topic in zookeeper is $offsetPath")
  }

  def getOffset(logType: String, offset: String): Map[TopicAndPartition, Long] = {
    if (zkClient == null || zkClient.getShutdownTrigger) {
      init(logType)
    }
    var fromOffsets: Map[TopicAndPartition, Long] = Map()

    val children = zkClient.countChildren(offsetPath)
    if (children > 0) {
      log.info(s"####topic($topic) has $children paritions...")
      val topicList = List(topic)
      val req = new TopicMetadataRequest(topicList, 0)
      val getLeaderConsumer = new SimpleConsumer(Functions.getHost(bootstrapServers), Functions.getPort(bootstrapServers), 10000, 10000, "OffsetLookup")
      val res = getLeaderConsumer.send(req)
      val topicMetaOption = res.topicsMetadata.headOption
      val partitions = topicMetaOption match {
        case Some(tm) =>
          tm.partitionsMetadata.map(pm => (pm.partitionId, pm.leader.get.host)).toMap[Int, String]
        case None =>
          Map[Int, String]()
      }
      if (offset.equalsIgnoreCase("earlest")) {
        log.info("#### the offset has been set to kafka smallest offset...")
        for (i <- 0 until children) {
          val tp = TopicAndPartition(topic, i)
          val requestMin = OffsetRequest(Map(tp -> PartitionOffsetRequestInfo(OffsetRequest.EarliestTime, 1)))
          val consumerMin = new SimpleConsumer(partitions(i), Functions.getPort(bootstrapServers), 10000, 10000, "getMinOffset")
          val curOffsets = consumerMin.getOffsetsBefore(requestMin).partitionErrorAndOffsets(tp).offsets
          fromOffsets += (tp -> curOffsets.head)
        }
      } else if (offset.equalsIgnoreCase("latest")) {
        log.info("#### the offset has been set to kafka largest offset...")
        for (i <- 0 until children) {
          val tp = TopicAndPartition(topic, i)
          val requestMax = OffsetRequest(Map(tp -> PartitionOffsetRequestInfo(OffsetRequest.LatestTime, 1)))
          val consumerMax = new SimpleConsumer(partitions(i), Functions.getPort(bootstrapServers), 10000, 10000, "getMaxOffset")
          val curOffsets = consumerMax.getOffsetsBefore(requestMax).partitionErrorAndOffsets(tp).offsets
          fromOffsets += (tp -> curOffsets.head)
        }
      } else {
        log.info("#### the offset is got from zk, and updated with kafka true offset...")
        for (i <- 0 until children) {
          val tp = TopicAndPartition(topic, i)
          val partitionOffset = zkClient.readData[String](s"$offsetPath/$i")
          val requestMin = OffsetRequest(Map(tp -> PartitionOffsetRequestInfo(OffsetRequest.EarliestTime, 1)))
          // -2,1
          val consumerMin = new SimpleConsumer(partitions(i), Functions.getPort(bootstrapServers), 10000, 10000, "getMinOffset")
          val curOffsets = consumerMin.getOffsetsBefore(requestMin).partitionErrorAndOffsets(tp).offsets
          var nextOffset = partitionOffset.toLong
          if (curOffsets.nonEmpty && nextOffset < curOffsets.head) {
            nextOffset = curOffsets.head
          }
          fromOffsets += (tp -> nextOffset)
        }
      }
    }else{
      log.info(s"####topic($topic) has no children in zookeeper..")
    }
    fromOffsets
  }

  def writeOffset(logType: String, offsetRanges: Array[OffsetRange]): Unit ={
    if (zkClient == null || zkClient.getShutdownTrigger) {
      init(logType)
    }
    for (o <- offsetRanges) {
      val zkPath = s"$offsetPath/${o.partition}"
      ZkUtils.updatePersistentPath(zkClient, zkPath, o.untilOffset.toString)
      log.info(s"#### topic:${o.topic}, partition:${o.partition}, fromoffset:${o.fromOffset}, untiloffset:${o.untilOffset} #######")
    }
  }

  def showOffset(logType: String, offsetRanges: Array[OffsetRange]): Unit ={
    if (zkClient == null || zkClient.getShutdownTrigger) {
      init(logType)
    }
    for (o <- offsetRanges) {
      //val zkPath = s"$offsetPath/${o.partition}"
      log.info(s"#### topic:${o.topic}, partition:${o.partition}, fromoffset:${o.fromOffset}, untiloffset:${o.untilOffset} #######")
    }
  }
}

object MyZkSerializer extends ZkSerializer
{
  override  def deserialize(bytes: Array[Byte]): String ={
    new String(bytes, Charsets.UTF_8)
  }
  override def serialize(data: scala.Any): Array[Byte] = {
    data.toString.getBytes(Charsets.UTF_8)
  }
}
