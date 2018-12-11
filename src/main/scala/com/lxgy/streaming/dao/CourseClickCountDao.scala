package com.lxgy.streaming.dao

import com.lxgy.spark.streaming.utils.HbaseUtils
import com.lxgy.streaming.domain.CourseClickCount
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer

/**
  * @author Gryant
  */
object CourseClickCountDao {

  val tableName = "course_click_count"
  val columnFamily = "info"
  val qualifer = "click_count"

  /**
    * 保存数据到Hbase
    *
    * @param list CourseClickCount集合
    */
  def save(list: ListBuffer[CourseClickCount]): Unit = {

    val table = HbaseUtils.newInstance().getTable(tableName);

    for (ele <- list) {
      table.incrementColumnValue(Bytes.toBytes(ele.dayCourse), Bytes.toBytes(columnFamily), Bytes.toBytes(qualifer), ele.clickCount)
    }
  }

  /**
    * 根据rowKey 查询对应的点击次数
    *
    * @param dayCourse
    */
  def count(dayCourse: String): String = {
    val table = HbaseUtils.newInstance().getTable(tableName);
    val value = table.get(new Get(Bytes.toBytes(dayCourse))).getValue(columnFamily.getBytes, qualifer.getBytes)
    if (value == null) {
      null
    }else{
      // TODO 异常：offset (0) + length (4) exceed the capacity of the array: 2
      // 当返回类型为 Long 时，可能会因为数据库中的值导致此异常
      Bytes.toString(value)
    }
  }

  def main(args: Array[String]): Unit = {

//    println("result:" + count("20181209_88"))
    println("result:" + count("20181207_145"))
  }

}
