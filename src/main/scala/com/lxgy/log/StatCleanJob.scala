package com.lxgy.log

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * @author Gryant
  */
object StatCleanJob {

  def main(args: Array[String]): Unit = {

//    val warehouseLocation = "/user/hive/warehouse"

    val spark = SparkSession
      .builder()
      .appName("StatCleanJob")
//      .config("spark.sql.warehouse.dir", warehouseLocation)
//      .enableHiveSupport()
      // 压缩格式调节
      .config("spark.sql.parquet.compression.codec","gzip")
      .master("local[2]")
      .getOrCreate()

    val inputPath = "/spark/data/access.log"
    val outputPath = "/spark/data/output/access_clean"

    val accessRDD: RDD[String] = spark.sparkContext.textFile(inputPath)
    //    rdd.take(10).foreach(x=> println(x))

    val accessDF: DataFrame = spark.createDataFrame(accessRDD.map(line => AccessConvertUtil.parseLog(line)), AccessConvertUtil.struct)
    //    df.printSchema()
    //    df.show(false)

    accessDF
      .coalesce(1) // 分区下文件的个数
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .partitionBy("day")
      .save(outputPath)

    spark.stop()

  }

}
