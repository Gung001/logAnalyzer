package com.lxgy.streaming.domain

/**
  * 清洗后的日志数据
  * @param ip IP地址
  * @param time 访问时间
  * @param courseId 课程比那好
  * @param statusCode 访问状态码
  * @param referer 访问来源
  */
case class ClickLog(ip: String, time: String, courseId: Int, statusCode: Int, referer: String)
