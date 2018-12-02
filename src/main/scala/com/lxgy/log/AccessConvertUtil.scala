package com.lxgy.log

import com.ggstar.util.ip.IpHelper
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

/**
  * 访问日志转换工具类
  * @author Gryant
  */
object AccessConvertUtil {

  val struct = StructType(Array(
    StructField("url", StringType),
    StructField("cmsType", StringType),
    StructField("cmsId", LongType),
    StructField("traffic", LongType),
    StructField("ip", StringType),
    StructField("city", StringType),
    StructField("time", StringType),
    StructField("day", StringType)
  ))

  /**
    * 根据输入数据每一行信息转换为输出的样式
    *
    * @param log
    */
  def parseLog(log: String) = {

    try {

      val splits = log.split("\t")

      val url = splits(1)
      val traffic = splits(2).toLong
      val ip = splits(3)

      val domain = "http://www.imooc.com/"
      val cms = url.substring(url.indexOf(domain) + domain.length)
      val cmsTypeId = cms.split("/")

      var cmsType = ""
      var cmsId = 0l;
      if (cmsTypeId.length > 1) {
        cmsType = cmsTypeId(0)
        cmsId = cmsTypeId(1).toLong
      }

      val city = IpHelper.findRegionByIp(ip)
      val time = splits(0)
      // yyyyMMdd
      val day = time.substring(0, 10).replaceAll("-", "")

      // Row 中的内容需要和 struct 对应
      Row(url, cmsType, cmsId, traffic, ip, city, time, day)
    } catch {
      case e: Exception => Row(0)
    }
  }
}
