package com.lxgy.log

import com.ggstar.util.ip.IpHelper

/**
  * IP解析工具类
  * @author Gryant
  */
object IpUtils {

  def getCiry(ip:String)= {
    IpHelper.findRegionByIp(ip)
  }

  def main(args: Array[String]): Unit = {
    println(getCiry("202.96.134.133"))
  }

}
