package etlutil

import java.util.regex.Pattern

import net.minidev.json.JSONObject
import org.apache.commons.logging.LogFactory

import scala.collection.JavaConversions.mutableMapAsJavaMap
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.math.random
import scala.util.control.Breaks._

/**
  * Created by SNOW on 2017/3/23.
  */

object Extract {
  private val log = LogFactory.getLog(Extract.getClass)
  var nameRegexps: Map[String, Pattern] = _
  var nameRegexps2: mutable.Buffer[(String, Pattern, String, String, String)] = _
  var extractMap: mutable.Map[String, Any] = _

  def map2Json(map: mutable.Map[String, Any]): String = {
    val jsonString = JSONObject.toJSONString(map)
    jsonString
  }

  def getRawRegexps(source: String): mutable.Buffer[String] = {
    getRawRegexps(source, fromDB = true)
  }

  def getRawRegexps(source: String, fromDB: Boolean): mutable.Buffer[String] = {
    var regexps = mutable.Buffer[String]()
    val pr = new PropertiesReader
    if (!fromDB) {
      pr.loadProperties("mysql.properties")
      breakable {
        for (i <- 1 to 1000) {
          val str = pr.getProperty(source + i)
          if (!str.equalsIgnoreCase("")) {
            regexps += str
          } else
            break
        }
      }
    } else {
      val mysqljdbcDao = new MysqlJDBCDao2
      val status = mysqljdbcDao.getStatus(source)
      if (status.equalsIgnoreCase("1")) {
        regexps = mysqljdbcDao.getRegexps(source).asScala
      } else {
        regexps.clear()
      }
      mysqljdbcDao.closeConn()
    }
    regexps
  }

  def extractRegexp(regexps: mutable.Buffer[String], hasMsg: Boolean): Map[String, Pattern] = {
    var nameRegexps = Map[String, Pattern]()
    val pattern = Pattern.compile("(\\(\\?[^)]*\\))")
    val subpattern = Pattern.compile("<(.*)>")
    if (regexps.nonEmpty) {
      for (regexp <- regexps) {
        val matcher = pattern.matcher(regexp)
        if (matcher.find()) {
          val submatcher = subpattern.matcher(matcher.group(1))
          if (submatcher.find()) {
            nameRegexps += (submatcher.group(1) -> Pattern.compile(regexp))
          } else {
            nameRegexps += ("Unknown" -> Pattern.compile(regexp))
          }
        }
      }
    }
    if (hasMsg) {
      nameRegexps += ("message" -> Pattern.compile("(?<message>.*)",Pattern.DOTALL))
    }
    nameRegexps
  }

  def extractRegexp(regexps: mutable.Buffer[String]): Map[String, Pattern] = {
    extractRegexp(regexps, hasMsg = true)
  }

  def extractRegexp2(regexps: mutable.Buffer[String], hasMsg: Boolean):
  mutable.Buffer[(String, Pattern, String, String, String)] = {
    val nameRegexps = mutable.Buffer[(String, Pattern, String, String, String)]()
    val pattern = Pattern.compile("(\\(\\?[^)]*\\))")
    val subpattern = Pattern.compile("<(.*)>")
    val splitPattern = Pattern.compile("@2@")
    var regStr = ""
    var typeStr = ""
    var funcStr = ""
    var paraStr = ""
    var length = 0
    if (regexps.size > 0) {
      for (regexp <- regexps) {
        regStr = ""
        typeStr = ""
        funcStr = ""
        paraStr = ""
        val splitArray = splitPattern.split(regexp)
        length = splitArray.length
        regStr = splitArray(0)
        if (length == 2) {
          funcStr = splitArray(1)
          paraStr = ""
        } else if (length == 3) {
          funcStr = splitArray(1)
          paraStr = splitArray(2)
        }
        //typeStr = if (length > 3) splitArray(3) else ""
        val matcher = pattern.matcher(regStr)
        if (matcher.find()) {
          val submatcher = subpattern.matcher(matcher.group(1))
          if (submatcher.find()) {
            nameRegexps.append((submatcher.group(1), Pattern.compile(regStr,Pattern.DOTALL), funcStr, paraStr, typeStr))
          } else {
            nameRegexps.append(("Unkown" + random, Pattern.compile(regStr,Pattern.DOTALL), funcStr, paraStr, typeStr))
          }
        }
      }
    }
    if (hasMsg) {
      nameRegexps.append(("message", Pattern.compile("(?<message>^.*$)",Pattern.DOTALL), "", "", "String"))
    }
    nameRegexps
  }

  def extractRegexp2(regexps: mutable.Buffer[String]):
  mutable.Buffer[(String, Pattern, String, String, String)] = {
    extractRegexp2(regexps, hasMsg = true)
  }

  def extract2Map(logString: String, nameRegexps: Map[String, Pattern]): mutable.Map[String, Any] = {
    val patterns = mutable.Map[String, Any]()
    for (nameregexp <- nameRegexps) {
      val matcher = nameregexp._2.matcher(logString)
      if (matcher.find()) {
        patterns += (nameregexp._1 -> matcher.group(nameregexp._1))
      }
      else {
        patterns += (nameregexp._1 -> "")
      }
    }
    val time = System.currentTimeMillis()
    val str = FunctionReflect.callFunction("dateFormatToString", time.toString, "13")
    patterns += ("@timestamp" -> str)
    patterns
  }

  def extract2Map2(logString: String, nameRegexps: mutable.Buffer[(String, Pattern, String, String, String)]): mutable.Map[String, Any] = {
    val patterns = mutable.Map[String, Any]()
    var res = ""
    for (nameregexp <- nameRegexps) {
      val matcher = nameregexp._2.matcher(logString)
      if (matcher.find()) {
        val str = matcher.group(nameregexp._1)
        if (!nameregexp._3.equalsIgnoreCase("")) {
          res = FunctionReflect.callFunction(nameregexp._3, str, nameregexp._4)
        } else {
          res = str
        }
        //patterns += (nameregexp._1 -> res)
        if(patterns.keySet.contains(nameregexp._1)){
          if(patterns.get(nameregexp._1).toString.equalsIgnoreCase("")){
            patterns.update(nameregexp._1,res)
          }
        }else{
          patterns += (nameregexp._1 -> res)
        }
      }
      else {
        //patterns += (nameregexp._1 -> "")
        if(!patterns.keySet.contains(nameregexp._1)){
          patterns += (nameregexp._1 -> "")
        }
      }
    }
    val time = System.currentTimeMillis()
    val str = FunctionReflect.callFunction("dateFormatToString", time.toString, "13")
    patterns += ("@timestamp" -> str)
    patterns
  }

  def log2json(logString: String, source: String, regexps: mutable.Buffer[String], hasMsg: Boolean, useFunciton: Boolean): String = {
    if (useFunciton)
      nameRegexps2 = extractRegexp2(regexps, hasMsg)
    else
      nameRegexps = extractRegexp(regexps, hasMsg)
    /*for (elem <- nameRegexps) {
      log.info(elem._1 +"-->"+ elem._2.toString)
    }*/
    /*needReGet = false
    log.info(s"####needReGet changed to false in Extract at ${System.currentTimeMillis()}")*/

    if (useFunciton)
      extractMap = extract2Map2(logString, nameRegexps2)
    else
      extractMap = extract2Map(logString, nameRegexps)
    /*for (elem <- extractMap) {
      log.info(s"++++++++++++++++${elem._1} = ${elem._2}")
    }*/
    /*val jsonStr = map2Json(extractMap)
    log.info(s"json string = $jsonStr")
    jsonStr*/
    map2Json(extractMap)
  }

  def log2json(logString: String, source: String, mapNameRegexps: mutable.Map[String, mutable.Buffer[String]], hasMsg: Boolean, useFunciton: Boolean): String = {
    val rawRegexps = mapNameRegexps.getOrElse(source, mutable.Buffer())
    log.info(s"####source = $source, rawRegexps.size = ${rawRegexps.size}")
    for (elem <- rawRegexps) {
      log.info(s"####the element of rawRegexps = $elem")
    }
    log2json(logString, source, rawRegexps, hasMsg, useFunciton)
  }

  def log2json(logString: String, source: String, fromDB: Boolean, hasMsg: Boolean, useFunciton: Boolean, needReGet: Boolean): String = {
    //log.info(s"####needRgGet = $needReGet")
    //log.info(s"####useFunciton = $useFunciton")
    if (needReGet) {
      val patterns = getRawRegexps(source, fromDB)
      if (useFunciton)
        nameRegexps2 = extractRegexp2(patterns, hasMsg)
      else
        nameRegexps = extractRegexp(patterns, hasMsg)
      /*for (elem <- nameRegexps) {
        log.info(elem._1 +"-->"+ elem._2.toString)
      }*/
      /*needReGet = false
      log.info(s"####needReGet changed to false in Extract at ${System.currentTimeMillis()}")*/
    }

    if (useFunciton)
      extractMap = extract2Map2(logString, nameRegexps2)
    else
      extractMap = extract2Map(logString, nameRegexps)
    /*for (elem <- extractMap) {
      log.info(s"++++++++++++++++${elem._1} = ${elem._2}")
    }*/
    /*val jsonStr = map2Json(extractMap)
    log.info(s"json string = $jsonStr")
    jsonStr*/
    map2Json(extractMap)
  }

  def log2json(logString: String, source: String, fromDB: Boolean, hasMsg: Boolean, useFunciton: Boolean): String = {
    log2json(logString, source, fromDB, hasMsg, useFunciton, needReGet = false)
  }

  def main(args: Array[String]): Unit = {
    //var patterns = List[String]()
    //patterns = patterns.::("(?<date>[\\d\\-]{10})")
    //patterns = patterns.::("(?<time>[\\d:]{8})")
    //patterns = patterns.::("\\|\\s+(?<threadNo>\\w+)\\s\\|")
    //val patterns = getRawRegexps("eventlog")
    //println(s"patterns.length = ${patterns.length}")

    val str = "1494491555657 | worker_11 | [heagle][TRST0003]外围系统批量记账结果查询[1745121987] 0s [0][]-[9998][TRST0003][2017022848287868]"
    //val str = "14-02-2017 外围系 23:58:34 | worker_11 | [heagle][TRST0003]外围系统批量记账结果查询[1745121987] 0s [0][]-[9998][TRST0003][2017022848287868]"
    //val str1 = "15-02-2017 12:32:39 | worker_9 | [heagle][TRST0003]外围系统批量记账结果查询[1845778779] 0s [0][]-[9998][TRST0003][2017021848306563]"
    /*for(pattern <- patterns){
    //for(i <- 0 until patterns.length){
      //val regexp = pattern.r
      //val res = regexp.findFirstIn(str)
      //val res = regexp.findAllIn(str).groupNames
      val regexp = Pattern.compile(pattern)
      val res = regexp.matcher(str)
      println("res.find() = " + res.find())
      println("res =" + res.group(1))
    }*/
    /*val a = extractRegexp(patterns)
    val b = extract2Map(str, a)
    b.foreach(u=>{
      println(s"groupName = ${u._1}, value = ${u._2}")
    })
    val resString = map2Json(b)
    println("resString = " + resString)*/

    //val res = log2json(str, "test", fromDB = false)
    //println(s"res = $res")
    /*val res1 = log2json(str1, "test", fromDB = false)
    println(s"res1 = $res1")*/

    /*val string = "(?<date>[\\d\\-]{10})"
    val string1 = "(?<date>[\\d\\-]{10})@2@dateFormatToString@2@13"
    var regexpsres = mutable.Buffer[(String, String, String)]()
    val values = string.split("@2@")
    val length = values.length
    var st1 = ""
    var st2 = ""
    var st3 = ""
    st1 = values(0)
    st2 = if (length > 1) values(1) else ""
    st3 = if (length > 2) values(2) else ""
    regexpsres.append((st1, st2, st3))
    val values1 = string1.split("@2@")
    val length1 = values1.length
    st1 = values1(0)
    st2 = if (length1 > 1) values1(1) else ""
    st3 = if (length1 > 2) values1(2) else ""
    regexpsres.append((st1, st2, st3))
    regexpsres.foreach(a =>
      println(a._1 + "---" + a._2 + "---" + a._3))*/
  }

}
