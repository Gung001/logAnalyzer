package com.lxgy.log

import java.util.{Date, Locale}

import org.apache.commons.lang3.time.FastDateFormat

/**
  * 时间转换工具类
  * SimpleDateFormat 是非线程安全的
  * @author Gryant
  */
object DateUtils {

  // 输入文件格式：10/Nov/2016:00:01:02 +0800
  val INPUT_FORMAT_DDMMYYYYHHMMSS = FastDateFormat.getInstance("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH)

  // 目标格式：2016-11-10 00:01:02
  val TARGET_FORMAT_YYYYMMDDHHMMSS = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")

  /**
    *
    * @param time
    */
  def getTime(time: String) = {
    try {
      INPUT_FORMAT_DDMMYYYYHHMMSS.parse(time.replace("[", "").replace("]", "")).getTime
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
    TARGET_FORMAT_YYYYMMDDHHMMSS.format(new Date(getTime(time)))
  }

  def main(args: Array[String]): Unit = {
    println(parse("[10/Nov/2016:00:01:02 +0800]"))
  }

}
