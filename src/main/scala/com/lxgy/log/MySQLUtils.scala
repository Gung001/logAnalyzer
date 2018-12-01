package com.lxgy.log

import java.sql.{Connection, DriverManager, PreparedStatement}

/**
  * @author Gryant
  */
object MySQLUtils {

  /**
    * 获取连接
    *
    * @return
    */
  def getConnection() = {
    DriverManager.getConnection("jdbc:mysql://data01/spark2?user=root&password=123456")
  }

  /**
    * 释放数据库连接资源
    *
    * @param conn
    * @param pstmt
    */
  def release(conn: Connection, pstmt: PreparedStatement) = {
    try {
      if (pstmt != null) {
        pstmt.close()
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (conn != null) {
        conn.close()
      }
    }
  }

  def main(args: Array[String]): Unit = {
    println(getConnection())
  }

}
