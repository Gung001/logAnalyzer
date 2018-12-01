package com.lxgy.log

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * 跑在yarn 上
  *
  * 出现异常：spark程序在yarn的集群运行，出现 Current usage: 105.9 MB of 1 GB physical memory used; 2.2 GB of 2.1 GB virtual memory used. Killing container. 错误
  * 解决：在etc/hadoop/yarn-site.xml文件中，修改检查虚拟内存的属性为false
  * <property>
  *   <name>yarn.nodemanager.vmem-check-enabled</name>
  *   <value>false</value>
  * </property>
  *
  * @author Gryant
  */
object StatCleanJobYarn {

  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      println("Usage:StatCleanJobYarn <inputPath> <outputPath>")
      System.exit(1)
    }

    val spark = SparkSession
      .builder()
      .getOrCreate()

    val Array(inputPath, outputPath) = args
//    val inputPath = "/spark/data/access.log"
//    val outputPath = "/spark/data/output/access_clean"

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
