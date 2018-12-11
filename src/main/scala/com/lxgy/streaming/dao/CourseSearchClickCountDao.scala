package com.lxgy.streaming.dao

import com.lxgy.spark.streaming.utils.HbaseUtils
import com.lxgy.streaming.domain.CourseSearchClickCount
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer

/**
  * @author Gryant
  */
object CourseSearchClickCountDao {

  val tableName = "course_search_click_count"
  val columnFamily = "info"
  val qualifer = "click_count"

  /**
    * 保存数据到Hbase
    *
    * @param list CourseSearchClickCount集合
    */
  def save(list: ListBuffer[CourseSearchClickCount]): Unit = {

    val table = HbaseUtils.newInstance().getTable(tableName);

    for (ele <- list) {
      table.incrementColumnValue(Bytes.toBytes(ele.daySearchCourse), Bytes.toBytes(columnFamily), Bytes.toBytes(qualifer), ele.clickCount)
    }
  }

  /**
    * 根据rowKey 查询对应的点击次数
    *
    * @param daySearchCourse
    */
  def count(daySearchCourse: String): Long = {
    val table = HbaseUtils.newInstance().getTable(tableName);
    val value = table.get(new Get(Bytes.toBytes(daySearchCourse))).getValue(columnFamily.getBytes, qualifer.getBytes)
    if (value == null) {
      0L
    } else {
      // TODO 异常：offset (0) + length (4) exceed the capacity of the array: 2
      // 当返回类型为 Long 时，可能会因为数据库中的值导致此异常
      Bytes.toLong(value)
    }
  }

  def main(args: Array[String]): Unit = {

    val list = new ListBuffer[CourseSearchClickCount]
    list.append(CourseSearchClickCount("20180101_www.baidu.com_10", 8))
    list.append(CourseSearchClickCount("20180101_cn.bing.com_11", 8))

    //save(list)

//    println("20180101_www.baidu.com_10:" + count("20180101_www.baidu.com_10"))
//    println("20180101_www.baidu.com_11:" + count("20180101_www.baidu.com_11"))
//    println("20181205_www.baidu.com_112:" + count("20181205_www.baidu.com_112"))
    println("20181207_www.sogou.com_145:" + count("20181207_www.sogou.com_145"))
    println("ol...........")


  }
}
