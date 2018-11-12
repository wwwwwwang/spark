package etlutil

/**
  * Created by SNOW on 2017/5/19.
  */

import akka.actor.{ActorSystem, Props, _}
import com.datageek.etl._
import org.apache.commons.logging.{Log, LogFactory}
import scala.collection.JavaConverters._

import scala.collection.mutable
import scala.concurrent.duration._

class MyActor extends Actor {
  val log: Log = LogFactory.getLog(classOf[MyActor])

  def receive: PartialFunction[Any, Unit] = {
    case (a,b) =>
      if(a.toString.equalsIgnoreCase("ETLWithKey1")){
        ETLWithKey1.needReGet = true
        log.info(s"ETLWithKey1.needReGet has been changed to ${ETLWithKey1.needReGet} by timer, time = ${System.currentTimeMillis()}")
        val mysqljdbcDao = new MysqlJDBCDao2
        ETLWithKey1.topKeys = mysqljdbcDao.getTopicKeys(b.toString)
        log.info(s"ETLWithKey1.topKeys has been changed to ${ETLWithKey1.topKeys} by timer, time = ${System.currentTimeMillis()}")
        mysqljdbcDao.closeConn()
      }else if(a.toString.equalsIgnoreCase("ETLWithKeyJson")){
        ETLWithKeyJson.keysAndIndexs.clear()
        val mysqljdbcDao = new MysqlJDBCDao2
        val topKeys = mysqljdbcDao.getTopicKeys(b.toString)
        log.info(s"ETLWithKeyJson.topKeys has been changed to $topKeys by timer, time = ${System.currentTimeMillis()}")
        if(!topKeys.equalsIgnoreCase("")){
          val keys = topKeys.split(",")
          for(key <- keys){
            var index : String = ""
            //val status = mysqljdbcDao.getStatus(key)
            //log.info(s"####the status of $key is $status")
            index = mysqljdbcDao.getIndex(key)
            log.info(s"####a map ($key,$index) is added to ETLWithKeyJson.keysAndIndexs")
            ETLWithKeyJson.keysAndIndexs += (key -> index)
          }
          log.info(s"####the size of keysAndIndexs in topic ${b.toString} is ${ETLWithKeyJson.keysAndIndexs.size}")
        }else{
          log.info("####ETLWithKeyJson.topKeys is equal to \"\"")
        }
        mysqljdbcDao.closeConn()
      }else if(a.toString.equalsIgnoreCase("ETLWithKeyText")){
        ETLWithKeyText.keysAndIndexs.clear()
        ETLWithKeyText.mapNameRegexps.clear()
        val mysqljdbcDao = new MysqlJDBCDao2
        val topKeys = mysqljdbcDao.getTopicKeys(b.toString)
        log.info(s"ETLWithKeyText.topKeys has been changed to $topKeys by timer, time = ${System.currentTimeMillis()}")
        if(!topKeys.equalsIgnoreCase("")){
          val keys = topKeys.split(",")
          for(key <- keys){
            var nameRegexps: mutable.Buffer[String] = mutable.Buffer()
            var index : String = ""
            val status = mysqljdbcDao.getStatus(key)
            log.info(s"####the status of $key is $status")
            if(status.equalsIgnoreCase("1")){
              nameRegexps = mysqljdbcDao.getRegexps(key).asScala
            }else{
              nameRegexps.clear()
            }
            index = mysqljdbcDao.getIndex(key)
            log.info(s"####a map ($key,${nameRegexps.size}) is added to ETLWithKeyText.mapNameRegexps")
            ETLWithKeyText.mapNameRegexps += (key->nameRegexps)
            log.info(s"####a map ($key,$index) is added to ETLWithKeyText.keysAndIndexs")
            ETLWithKeyText.keysAndIndexs += (key -> index)
          }
          log.info(s"####the size of mapNameRegexps in topic ${b.toString} is ${ETLWithKeyText.mapNameRegexps.size}")
          log.info(s"####the size of keysAndIndexs in topic ${b.toString} is ${ETLWithKeyText.keysAndIndexs.size}")
        }else{
          log.info("####ETLWithKeyText.topKeys is equal to \"\"")
        }
        mysqljdbcDao.closeConn()
      }else if(a.toString.equalsIgnoreCase("ETL")){
        ETL.nameRegexps = Extract.getRawRegexps(b.toString)
        log.info(s"ETL.nameRegexps has been changed by timer, size =  ${ETL.nameRegexps.size} , time = ${System.currentTimeMillis()}")
      }else{
        log.info(s"wrong class name")
      }
    case _ => log.info(s"wrong parameters received!")
  }
}

class ThreadDemo(caseName: String, topicName: String) extends Runnable {
  val name: String = caseName
  val topic: String = topicName
  override def run() {
    Timer.changeValue((name, topic))
  }
}

object Timer {
  var length: Long = 30
  val Actorsystem: ActorSystem = ActorSystem.create("Timer")
  val act: ActorRef = Actorsystem.actorOf(Props[MyActor], "changeValue")
  //implicit val time = Timeout(5 seconds)

  def changeValue(nameTopic: (String, String)): Unit = {
    import scala.concurrent.ExecutionContext.Implicits.global
    Actorsystem.scheduler.schedule(0 milliseconds, new FiniteDuration(length, SECONDS), act, nameTopic)
  }

  def main(args: Array[String]): Unit = {
    import scala.concurrent.ExecutionContext.Implicits.global
    //1.system.scheduler.scheduleOnce(2 seconds, act1, "foo")
    /*2.system.scheduler.scheduleOnce(2 seconds){ act1 ? "Hello" }*/
    //3.这将会计划0ms后每50ms向tickActor发送 Tick-消息
    val cancellable = Actorsystem.scheduler.schedule(0 milliseconds, 5 seconds, act, 0)
    //这会取消未来的Tick发送
    //cancellable.cancel()
  }

}
