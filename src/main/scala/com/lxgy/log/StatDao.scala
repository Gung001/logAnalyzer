package com.lxgy.log

import java.sql.{Connection, PreparedStatement}

import scala.collection.mutable.ListBuffer

/**
  * 各类统计入库操作
  *
  * @author Gryant
  */
object StatDao {

  def insertDayVideoAccessTopN(list: ListBuffer[DayVideoAccessStat]) = {

    var connection: Connection = null
    var pstmt: PreparedStatement = null

    try {
      connection = MySQLUtils.getConnection()

      // 关闭自动提交
      connection.setAutoCommit(false)

      val sql = "insert into day_video_access_topn_stat(day,cms_id,times) values (?,?,?)"
      pstmt = connection.prepareStatement(sql)

      for (ele <- list) {
        pstmt.setString(1, ele.day)
        pstmt.setLong(2, ele.cmsId)
        pstmt.setLong(3, ele.times)

        // 添加到批次中
        pstmt.addBatch()
      }

      // 批次处理
      pstmt.executeBatch()

      // 统一提交事务
      connection.commit()
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      MySQLUtils.release(connection, pstmt)
    }
  }

  def insertDayCityVideoAccessTopN(list: ListBuffer[DayCityVideoAccessStat]) = {

    var connection: Connection = null
    var pstmt: PreparedStatement = null

    try {
      connection = MySQLUtils.getConnection()

      // 关闭自动提交
      connection.setAutoCommit(false)

      val sql = "insert into day_video_city_access_topn_stat(day,cms_id,city,times,times_rank) values (?,?,?,?,?)"
      pstmt = connection.prepareStatement(sql)

      for (ele <- list) {
        pstmt.setString(1, ele.day)
        pstmt.setLong(2, ele.cmsId)
        pstmt.setString(3, ele.city)
        pstmt.setLong(4, ele.times)
        pstmt.setInt(5, ele.timesRank)

        // 添加到批次中
        pstmt.addBatch()
      }

      // 批次处理
      pstmt.executeBatch()

      // 统一提交事务
      connection.commit()
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      MySQLUtils.release(connection, pstmt)
    }
  }


  def insertVideoTrafficsAccessTopN(list: ListBuffer[DayVideoTrafficsAccessStat]) = {

    var connection: Connection = null
    var pstmt: PreparedStatement = null

    try {
      connection = MySQLUtils.getConnection()

      // 关闭自动提交
      connection.setAutoCommit(false)

      val sql = "insert into day_video_traffics_topn_stat(day,cms_id,traffics) values (?,?,?)"
      pstmt = connection.prepareStatement(sql)

      for (ele <- list) {
        pstmt.setString(1, ele.day)
        pstmt.setLong(2, ele.cmsId)
        pstmt.setLong(3, ele.traffics)

        // 添加到批次中
        pstmt.addBatch()
      }

      // 批次处理
      pstmt.executeBatch()

      // 统一提交事务
      connection.commit()
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      MySQLUtils.release(connection, pstmt)
    }
  }

  def deleteData(date:String) = {

    var connection: Connection = null
    var pstmt: PreparedStatement = null
    val tables = Array(
      "day_video_access_topn_stat",
      "day_video_city_access_topn_stat",
      "day_video_traffics_topn_stat"
    )

    try {
      connection = MySQLUtils.getConnection()

      for (table <- tables) {

        val sql = s"delete from $table where day = ?"
        pstmt = connection.prepareStatement(sql)

        pstmt.setString(1, date)

        pstmt.executeUpdate()
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      MySQLUtils.release(connection, pstmt)
    }
  }

}
