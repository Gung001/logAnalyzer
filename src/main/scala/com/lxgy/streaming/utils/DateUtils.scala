package com.lxgy.streaming.utils

import java.util.Date

import org.apache.commons.lang3.time.FastDateFormat

/**
  * 时间转换工具类
  * @author Gryant
  */
object DateUtils {

  val YYYYMMDDHHMMSS_FORMAT = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")

  val TARGET_FORMAT = FastDateFormat.getInstance("yyyyMMddHHmmss")

  /**
    * 获取时间戳
    * @param time
    */
  def getTime(time: String) = {
    try {
      YYYYMMDDHHMMSS_FORMAT.parse(time).getTime
    } catch {
      case e: Exception => {
        0l
      }
    }
  }

  /**
    * 获取时间：yyyy-MM-dd HH:mm:ss
    *
    * @param time
    */
  def parse(time: String) = {
    TARGET_FORMAT.format(new Date(getTime(time)))
  }

  def main(args: Array[String]): Unit = {
    println(parse("2018-12-05 16:51:01"))
  }

}
