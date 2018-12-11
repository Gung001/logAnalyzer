package com.lxgy.streaming.domain

/***
  * 课程点击数
  * @param dayCourse 对应的就是Hbase中的rowkey，20181209_1
  * @param clickCount 20181209_1 对应的访问总数
  */
case class CourseClickCount(dayCourse: String, clickCount: Long)
