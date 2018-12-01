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
}
