package com.lxgy.sql

import java.util.Properties

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * @author Gryant
  */
object HiveJoinMySQLData {

  def main(args: Array[String]): Unit = {

    val warehouseLocation = "/user/hive/warehouse"

    val spark = SparkSession
      .builder()
      .appName("HiveJoinMySQLData")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .master("local[*]")
      .getOrCreate()

    val url = "jdbc:mysql://data01/spark2"
    val username = "root"
    val password = "123456"
    val table = "t_dept"
    val props = new Properties()
    props.put("user", username)
    props.put("password", password)

    // hive 表数据写入到MySQL中
    spark.read
      .table("common.dept")
      .write
      .mode(SaveMode.Overwrite)
      .jdbc(url, table, props)

    // 读取MySQL数据形成临时表
    val deptDF = spark
      .read
      .jdbc(url, table, props)
//      .jdbc(url, table,"deptno",10,30,4, props)
//      .jdbc(url, table,Array("deptno < 20","deptno >= 20 and deptno < 30","deptno >= 30"), props)
//    deptDF.show()
    deptDF.createOrReplaceTempView("tmp_t_dept")

    // 数据聚合
    spark.sql(
      """
        |select ttd.dname,e.*
        |from common.emp e
        |join tmp_t_dept ttd on e.deptno = ttd.deptno
      """.stripMargin)
      .createOrReplaceTempView("tmp_emp_join_dept_result")

    // cache 视图
    spark.catalog.cacheTable("tmp_emp_join_dept_result")

//    spark.sql(
//      """
//        |select * from tmp_emp_join_dept_result
//      """.stripMargin).show()

    // 写数据到HDFS
    spark
      .read
      .table("tmp_emp_join_dept_result")
      .write
      .mode(SaveMode.Overwrite)
      .save("/spark/sql/data/hive_mysql_join")

    // 数据保存到hive中，并设置为parquet格式和分区字段
    spark
      .read
      .table("tmp_emp_join_dept_result")
      .write
      .format("parquet")
      .partitionBy("deptno")
      .mode(SaveMode.Overwrite)
      .saveAsTable("common.hive_emp_dept")

    spark
      .read
      .table("common.hive_emp_dept")
      .show(1)

    // uncache 视图
    spark.catalog.uncacheTable("tmp_emp_join_dept_result")
  }
}
