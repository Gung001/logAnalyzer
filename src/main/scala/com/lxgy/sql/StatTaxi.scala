package com.lxgy.sql

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
  * 从csv 文件中读取数据，并取出各个时间段内载客次数前五的出租车
  *
  * @author Gryant
  */
object StatTaxi {

  def main(args: Array[String]): Unit = {

    val warehouseLocation = "/user/hive/warehouse"
    val spark = SparkSession
      .builder()
      .appName("StatTaxi")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("spark.sql.shuffle.partitions", 10)
      .enableHiveSupport()
      .master("local[*]")
      .getOrCreate()

   // 定义输入输出文件路径
    val inputPath = "/spark/data/taxi.csv"
    val outputPath = "/spark/data/output/taxi"

   // 定义数据的schema
    val schema = StructType(Array(
      StructField("id", IntegerType),
      StructField("lat", StringType),
      StructField("lon", StringType),
      StructField("time", StringType)))

   // 读取数据
    spark.read
      .option("header", "false") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .schema(schema) // 指定schema
      .csv(inputPath)
      .createOrReplaceTempView("taxi_tmp")
//      .show()

    // 数据清洗-->得到时间点
    spark.sql(
      """
        |select id,lat,lon,SUBSTRING(time,0,2) hour from taxi_tmp
      """.stripMargin)
        .createOrReplaceTempView("taxi_hour_tmp")
//      .show(false)

    // 执行数据统计
    spark.sql(
      """
        |select
        |id,
        |hour,
        |count(1) count
        |from taxi_hour_tmp
        |group by id,hour
      """.stripMargin)
        .createOrReplaceTempView("taxi_count_tmp")
//      .show(false)

    // 取每个时间段排名前三的出租车及其单数
    val resultDF = spark.sql(
      """
        |select * from (
        |select id,hour,count,
        |row_number() over ( partition by hour order by count desc) rank
        |from taxi_count_tmp
        |) t
        |where t.rank <=5
        |order by hour asc
      """.stripMargin)
//      .show(false)

    // 数据输出
    resultDF
      .repartition(1) // 重分区，以一个分区输出文件
      .write
      .option("header", "true") // Use first line of all files as header
      .mode(SaveMode.Overwrite)
      .csv(outputPath)
  }
}
