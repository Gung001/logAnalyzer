package com.lxgy.core

import org.apache.spark.{AccumulableParam, SparkConf, SparkContext}
import scala.collection.mutable

/**
  * @author Gryant
  */
object WordCount {

  def main(args: Array[String]): Unit = {

    // 1，创建Spark Context上下文
    val sparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    var path = "/spark/data/wc.txt"

    val rdd = sc.textFile(path)

    /*val result1 = rdd
      .flatMap(line => line.split(" "))
      .filter(_.nonEmpty)
      .map((_, 1))
      .reduceByKey(_ + _)
    println("==result1:" + result1.collect().mkString(";"))*/

    val specialCharts = Array(";", "；", "\\.", "!", "'", ",")

    /**
      * 业务需要：
      * 将所有的单词转换为小写
      * 去掉所有单词中的特殊字符，然后再进行统计
      */
    /*val result2 = rdd.map(line => {
      var tline = line.toLowerCase()
      specialCharts.foreach(character=> {
        tline = tline.replaceAll(character, "")
      })
      tline
    })
      .flatMap(line => line.split(" "))
      .filter(_.nonEmpty)
      .map((_, 1))f
      .reduceByKey(_ + _)
    println("==result2:" + result2.collect().mkString(";"))*/


    /**
      * 技术优化：使用广播变量
      */
    val specialChartsBroadcast = sc.broadcast(specialCharts)
    val result3 = rdd.map(line => {
      var tline = line.toLowerCase()
      specialChartsBroadcast.value.foreach(character => {
        tline = tline.replaceAll(character, "")
      })
      tline
    })
      .flatMap(line => line.split(" "))
      .filter(_.nonEmpty)
      .map((_, 1))
      .reduceByKey(_ + _)
    println("==result3:" + result3.collect().mkString(";"))

    /**
      * 需求
      * 计算总行数&总单词数
      */
    println(s"-总行数：${rdd.count()}")
    println(s"-总单词数：${rdd.flatMap(_.split(" ")).count()}")

    /**
      * 需求
      * 使用累加器计算总行数&总单词数
      */
    var totalLines = sc.accumulator(0, "total_lines");
    var totalWords = sc.accumulator(0, "total_words");

    rdd.foreach(line => {

      totalLines += 1

      line.split(" ").foreach(word => {
        totalWords += 1
      })
    })

    println(s"=总行数：${totalLines}")
    println(s"=总单词数：${totalWords}")

    /**
      * 需求
      * 统计原始wordcount以及去掉特殊字符的wordcount程序
      */
    // TODO 隐式转换需加强
    implicit val mapOfAccumulator = AccumulableParamDemo

    val wordCountMap1 = sc.accumulable(mutable.Map[String, Int]())
    //(param = AccumulableParamDemo)
    val wordCountMap2 = sc.accumulable(mutable.Map[String, Int]())
    //(param = AccumulableParamDemo)

    rdd.foreachPartition(iter => {
      iter.foreach(line => {
        var tline = line.toLowerCase
        specialChartsBroadcast.value.foreach(character => {
          tline = tline.replaceAll(character, "")
        })

        // 过滤后的单词统计
        tline.split(" ").foreach(word => {
          wordCountMap1 += word -> 1
        })

        // 原串统计
        line.split(" ").foreach(word => {
          wordCountMap2 += word -> 1
        })
      })
    })
    println(s"原始串结果：${wordCountMap2.value.mkString(";")}")
    println(s"过滤串结果：${wordCountMap1.value.mkString(";")}")
  }
}

/**
  * 隐式转换参数
  */
object AccumulableParamDemo extends AccumulableParam[mutable.Map[String, Int], (String, Int)] {
  override def addAccumulator(r: mutable.Map[String, Int], t: (String, Int)): mutable.Map[String, Int] = {
    // 将t对象添加到累加器对象r中
    val v = r.getOrElse(t._1, 0) + t._2 // 获取值
    r(t._1) = v // 更新值
    r // 返回结果
  }

  override def addInPlace(r1: mutable.Map[String, Int], r2: mutable.Map[String, Int]): mutable.Map[String, Int] = {

    // 合并两个累加器对象，再driver中触发，也能够再executor 中触发（多个task执行结果的合并）
    r1.foldLeft(r2)((r, t) => {
      val v = r.getOrElse(t._1, 0) + t._2 // 获取值
      r(t._1) = v // 更新值
      r // 返回结果
    })
  }

  override def zero(initialValue: mutable.Map[String, Int]): mutable.Map[String, Int] = {
    // 初始化方法
    initialValue
  }
}