package com.lxgy.log

/**
  * 每天每个城市课程访问次数实体类
  *
  * @author Gryant
  */
case class DayCityVideoAccessStat(day: String, cmsId: Long, city: String, times: Long, timesRank: Int)
