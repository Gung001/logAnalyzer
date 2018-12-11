package com.lxgy.streaming

import com.lxgy.streaming.dao.{CourseClickCountDao, CourseSearchClickCountDao}
import com.lxgy.streaming.domain.{ClickLog, CourseClickCount, CourseSearchClickCount}
import com.lxgy.streaming.utils.DateUtils
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

/**
  *
  * 统计数据：数据源从Kafka中来
  *
  * @author Gryant
  */
object StatDataFromKafka {

  def main(args: Array[String]): Unit = {

    if (args.length != 4) {
      System.err.println("Usage: StatDataFromKafka <zkQuorum> <groupId> <topics> <numThreads>")
      System.exit(1)
    }

    val Array(zkQuorum, groupId, topics, numThreads) = args

    val sparkConf = new SparkConf()
      .setAppName("StatDataFromKafka")
      .setMaster("local[4]")

    val ssc = new StreamingContext(sparkConf, Seconds(60))

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap

    val messages = KafkaUtils.createStream(ssc, zkQuorum, groupId, topicMap)

    // 测试步骤一：测试数据接收
//     messages.map(_._2).count().print()

    // 测试步骤二：数据清洗
    val result: DStream[ClickLog] = messages.map(_._2).map(line => {
      // 10.29.55.98	2018-12-05 17:01:01	"GET /class/131.html HTTP/1.1"	200	http://cn.bing.com/search?q=Hadoop基础
      val infos = line.split("\t")

      // "GET /class/131.html HTTP/1.1"
      val url = infos(2).split(" ")(1)

      // /class/131.html
      // 实战课程编号
      var courseId = 0
      if (url.startsWith("/class")) {
        // 131.html
        val courseIdHTML = url.split("/")(2)
        courseId = courseIdHTML.split("\\.")(0).toInt
      }

      ClickLog(infos(0), DateUtils.parse(infos(1)), courseId, infos(3).toInt, infos(4))
    })

//     result.print()

    // 测试步骤三：统计今天到现在为止实战课程的访问量
    result.map(x => {
      // HBase rowkey 设计： 20181209_88
      (x.time.substring(0, 8) + "_" + x.courseId, 1)
    })
      .reduceByKey(_ + _)
      .foreachRDD(rdd => {
        rdd.foreachPartition(partitionRecord => {

          val list = new ListBuffer[CourseClickCount]
          partitionRecord.foreach(pair => {
            println("-----------CourseClickCount,key:" + pair._1 + ",value:" + pair._2)
            list.append(CourseClickCount(pair._1, pair._2))
          })

          CourseClickCountDao.save(list)
        })
      })

    // 测试步骤四：统计从搜索引擎过来的今天到现在为止实战课程的访问量
    result.map(x => {

      // https://www.sogou.com/web?query=Storm实战
      val referer = x.referer.replaceAll("//", "/")
      // https:/www.sogou.com/web?query=Storm实战
      val splits = referer.split("/")
      var host = ""
      // [https:,www.sogou.com,web?query=Storm实战]
      if (splits.length > 2) {
        host = splits(1)
      }

      // HBase rowkey 设计： 20181209_cn.bing.com_88
      if (host == "") {
        ("", 0)
      } else{
        (x.time.substring(0, 8) + "_" + host + "_" + x.courseId, 1)
      }
    }).filter(_._1 != "")
      .reduceByKey(_ + _)
      .foreachRDD(rdd => {
        rdd.foreachPartition(partitionRecord => {

          val list = new ListBuffer[CourseSearchClickCount]
          partitionRecord.foreach(pair => {
            println("=============CourseSearchClickCount,key:" + pair._1 + ",value:" + pair._2)
            list.append(CourseSearchClickCount(pair._1, pair._2))
          })

          CourseSearchClickCountDao.save(list)
        })
      })

    ssc.start()
    ssc.awaitTermination()
  }
}
