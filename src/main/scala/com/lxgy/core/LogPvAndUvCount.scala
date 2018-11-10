package com.lxgy.core

import java.sql.DriverManager

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * PV：PageView，网站浏览量
  * UV：用户数，去重（guid）
  * VV：访客的访问次数
  * IP：独立IP数
  *
  * @author Gryant
  */
object LogPvAndUvCount {

  val url = "jdbc:mysql://data01:3306/spark2?characterEncoding=utf-8"
  val user = "root"
  val pwd = "123456"


  def main(args: Array[String]): Unit = {

    /** *
      * 分析PV/UV逻辑
      *
      *
      */

    // 1，创建Spark Context上下文
    val sparkConf = new SparkConf()
//      .setMaster("local")
      .setAppName("LogPvAndUvCount")
    val sc = new SparkContext(sparkConf)

    // 2.基于sc构建RDD
    //    val path = "hdfs://data01:8020/spark/data/page_views.data"
    val path = "/spark/data/page_views.data"
    val outputPathDir = "/spark/core/output/"
    val rdd = sc.textFile(path)

    // 3.业务实现

    val mapredRdd = rdd.map(line => line.split("\t"))
      .filter(arr => {
        arr.length >= 3 && arr(0).trim.length > 0
      })
      .map(arr => {
        val date = arr(0).substring(0, 10)
        val url = arr(1).trim
        val guid = arr(2).trim
        (date, url, guid)
      })

    // 缓存
    mapredRdd.cache()

    /**
      * PV计算
      */
    val filteredPVRdd = mapredRdd.filter(t => t._2.nonEmpty).map(t => (t._1, 1))
    /*val filteredPVRdd = rdd.map(line => line.split("\t"))
      .filter(arr => {
        arr.length >= 2 && arr(1).nonEmpty && arr(0).trim.length > 0
      })
      .map(arr => {
        val date = arr(0).substring(0, 10)
        // val url = arr(1).trim
        (date, 1)
      })

    // groupByKey计算（不推荐）
    filteredPVRdd.groupByKey().map(line=>{
      val date = line._1
      val urlCount = line._2.size
      (date,urlCount)
    })*/

    // reduceByKey 计算
    val pvRdd: RDD[(String, Int)] = filteredPVRdd reduceByKey (_ + _)
    println("===============pv:" + pvRdd.collect().mkString(","))
    // ===============pv:(2013-05-19,100000)

    /**
      * UV计算
      */
    val filteredUVRdd = mapredRdd.filter(t => t._3.nonEmpty).map(t => (t._1, t._3))
    /*val filteredUVRdd: RDD[(String, String)] = rdd.map(line => line.split("\t"))
      .filter(arr => {
        arr.length >= 2 && arr(2).nonEmpty && arr(0).trim.length > 0
      })
      .map(arr => {
        val date = arr(0).substring(0, 10)
         val guid = arr(2).trim
        (date, guid)
      })

    // 基于groupByKey计算
    filteredUVRdd
      .groupByKey()
      .map(t=>{
        val date = t._1
        val guids = t._2

        val uv = guids.toSet.size // set 集合是一个无序的唯一的集合
        (date,uv)
      })

    // 基于reduceByKey计算
    filteredUVRdd
      .map(t=>{((t._1,t._2),null)})
      .reduceByKey((a,b)=>a)
      .map(t=>(t._1._1,1))
      .reduceByKey(_+_)
  */
    // 基于reduceByKey计算&distinct
    val uvRdd = filteredUVRdd
      .distinct()
      .map(t => (t._1, 1))
      .reduceByKey(_ + _)
    println("===============uv:" + uvRdd.collect().mkString(","))
    // ===============uv:(2013-05-19,47903)

    /**
      * 最终指标合并
      */
    val pvuvRdd: RDD[(String, Int, Int)] = pvRdd.fullOuterJoin(uvRdd)
      .map(t => {
        val date = t._1
        val pv = t._2._1.getOrElse(0)
        val uv = t._2._2.getOrElse(0)
        (date, pv, uv)
      })
    println("===============pvuv:" + pvuvRdd.collect().mkString(","))
    // ===============pvuv:(2013-05-19,100000,47903)

    // 结果数据输出（Driver/HDFS/RDBMS）
    // Driver(要求：Driver的内存需要可以放下这么多的数据，返回的数据大小不能超过spark.driver.maxResultSize的值，默认为1G)
    val result = pvuvRdd.collect()
    // HDFS
    pvuvRdd.saveAsTextFile(outputPathDir + s"pv_uv/${System.currentTimeMillis()}")
    // RDBMS
    pvuvRdd.foreachPartition(iter => {

      // 创建数据库连接对象
      val conn = DriverManager.getConnection(url, user, pwd)
      // 创建预编译对象
      val pstmt = conn.prepareStatement("insert into pv_uv values(?,?,?,?,?)")

      // 数据迭代输出
      iter.foreach(t => {
        val date = t._1
        val pv = t._2
        val uv = t._3

        pstmt.setLong(1, 0)
        pstmt.setString(2, date)
        pstmt.setInt(3, pv)
        pstmt.setInt(4, uv)
        pstmt.setLong(5, System.currentTimeMillis())

        pstmt.executeUpdate()
      })

      // 关闭数据连接
      if (pstmt == null) {
        pstmt.close()
      }
      if (conn == null) {
        conn.close()
      }

    })

    // 休眠
    Thread.sleep(10000)

  }

}
