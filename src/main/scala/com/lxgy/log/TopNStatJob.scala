package com.lxgy.log

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer

/**
  * @author Gryant
  */
object TopNStatJob {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("StatCleanJob")
      // 调整分区字段数类型，默认true
      .config("spark.sql.sources.partitionColumnTypeInference.enabled", false)
      .enableHiveSupport()
      .master("local[2]")
      .getOrCreate()

    val inputPath = "/spark/data/output/access_clean"
    val accessDF: DataFrame = spark.read.format("parquet").load(inputPath)
    //    accessDF.printSchema()
    //    accessDF.show(false)

    val currentDay = "20170511"

    // 删除当天的数据
    StatDao.deleteData(currentDay)

    // 最受欢迎的topN课程
    videoAccessTopNStat(spark, accessDF, currentDay)

    // 按照地市统计topN课程
    cityAccessTopNStat(spark, accessDF, currentDay)

    // 按照流量统计topN课程
    videoTrafficsTopNStat(spark, accessDF, currentDay)

    spark.stop()
  }

  /**
    * 最受欢迎的topN课程
    *
    * @param spark
    * @param accessDF
    */
  def videoAccessTopNStat(spark: SparkSession, accessDF: DataFrame, currentDay: String) = {

    // 方式一：使用 DSL 语法实现
    //    import spark.implicits._
    //    val videoAccessTopN = accessDF
    //      .filter(s"day = '20170511' and cmsType = 'video'")
    //      .groupBy("day", "cmsId")
    //      .agg(count("cmsId").as("times"))
    //      .orderBy($"times".desc)

    accessDF.createOrReplaceTempView("access_log_tmp")

    val videoAccessTopN = spark.sql(
      s"""
         | select day,cmsId,count(0) times
         | from access_log_tmp
         | where day = '$currentDay' and cmsType = 'video'
         | group by day,cmsId
         | order by times desc
      """.stripMargin)

    videoAccessTopN.show(false)

    // 数据写入MySQL
    videoAccessTopN.foreachPartition(record => {
      val list = new ListBuffer[DayVideoAccessStat]

      record.foreach(r => {
        val day = r.getAs[String]("day")
        val cmsId = r.getAs[Long]("cmsId")
        val times = r.getAs[Long]("times")

        list.append(DayVideoAccessStat(day, cmsId, times))
      })

      StatDao.insertDayVideoAccessTopN(list)
    })
  }

  /**
    * 按照地市统计topN课程
    *
    * @param spark
    * @param accessDF
    */
  def cityAccessTopNStat(spark: SparkSession, accessDF: DataFrame, currentDay: String) = {

    val cityAccessTopN = accessDF
      .filter(s"day = '$currentDay' and cmsType = 'video'")
      .groupBy("day", "city", "cmsId")
      .agg(count("cmsId").as("times"))

    // Window 函数在SparkSQL中的使用
    val resultDF = cityAccessTopN
      .select(
        cityAccessTopN("day"),
        cityAccessTopN("city"),
        cityAccessTopN("cmsId"),
        cityAccessTopN("times"),
        row_number().over(Window.partitionBy(cityAccessTopN("city")).orderBy(cityAccessTopN("times").desc)).as("times_rank")
      )
      .filter("times_rank <= 3")

    // 数据写入MySQL
    resultDF.foreachPartition(record => {
      val list = new ListBuffer[DayCityVideoAccessStat]

      record.foreach(r => {
        val day = r.getAs[String]("day")
        val cmsId = r.getAs[Long]("cmsId")
        val city = r.getAs[String]("city")
        val times = r.getAs[Long]("times")
        val timesRank = r.getAs[Int]("times_rank")

        list.append(DayCityVideoAccessStat(day, cmsId, city, times, timesRank))
      })

      StatDao.insertDayCityVideoAccessTopN(list)

      // 出现异常：java.sql.BatchUpdateException: Incorrect string value: '\xE5\xAE\x89\xE5\xBE\xBD...' for column 'city' at row 1
      // 解决：设置字段的字符集为utf8


    })
  }

  /**
    * 按照流量统计
    *
    * @param spark
    * @param accessDF
    */
  def videoTrafficsTopNStat(spark: SparkSession, accessDF: DataFrame, currentDay: String) = {

    import spark.implicits._
    val trafficsAccessTopN = accessDF
      .filter(s"day = '$currentDay' and cmsType = 'video'")
      .groupBy("day", "cmsId")
      .agg(sum("traffic").as("traffics"))
      .orderBy($"traffics".desc)
    //      .show(false)

    // 数据写入MySQL
    trafficsAccessTopN.foreachPartition(record => {
      val list = new ListBuffer[DayVideoTrafficsAccessStat]

      record.foreach(r => {
        val day = r.getAs[String]("day")
        val cmsId = r.getAs[Long]("cmsId")
        val traffics = r.getAs[Long]("traffics")

        list.append(DayVideoTrafficsAccessStat(day, cmsId, traffics))
      })

      StatDao.insertVideoTrafficsAccessTopN(list)
    })
  }
}
