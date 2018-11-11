package com.lxgy.core

import org.apache.spark.rdd.{RDD, ShuffledRDD}
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

import scala.util.Random

/**
  * @author Gryant
  */
object GroupSortedTopN {

  def main(args: Array[String]): Unit = {

    // topN
    val K = 3

    // 配置文件
    val conf = new SparkConf()
      .setAppName("GroupSortedTopN")
      .setMaster("local")

    // 只能存在一个Driver
    val sc = SparkContext.getOrCreate(conf)

    // 读取数据
    val path = "/spark/data/groupsort.txt"
    val rdd = sc.textFile(path)

    // 将RDD转化为key/value键值对RDD，并缓存
    val mapredRDD: RDD[(String, Int)] = rdd.map(line => line.split(" "))
      .filter(arr => arr.length == 2)
      .map(arr => (arr(0).trim, arr(1).trim.toInt))
      .cache()

    /** 分区topN实现方式一：通过groupByKey实现
      * 缺点：
      * 相同的key 的数据形成迭代器再后续的处理中会全部加载到内存中，所以再groupByKey的过程中，如果一个key 的数量特别多，可能会出现数据倾斜或OOM异常
      * 在同组key进行聚合的业务场景中，groupByKey性能很低，因为groupByKey没有先进行分区数据的聚合（减少shuffle的数据量），在聚合的业务场景中，一部分数据是不需要考虑的，而groupByKey会将所有的数据进行shuffle操作
      */
    val result1: RDD[(String, List[Int])] = mapredRDD.groupByKey().map(line => {
      val values = line._2
      val topKValues = values.toList.sorted.takeRight(K).reverse
      (line._1, topKValues)
    })
    println(s"分区topN实现方式一结果：\n" + result1.collect().mkString("\n"))

    /** 分区topN实现方式二：解决groupByKey缺点1==》分两个阶段聚合
      * 思路：
      * 第一步：给key 加一个随机前缀i
      * 第二部：将随机前缀去掉，然后做一个全局聚合
      *
      * 注意：
      * Scala2.11.0版本之前的random是不可以进行序列化操作的
      *
      */
    val result2 = mapredRDD.mapPartitions(iter => {
      val random = Random
      iter.map(t => ((random.nextInt(10), t._1), t._2))
    })
      .groupByKey()
      .flatMap {
        case ((prefix, key), values) => {
          values.toList.sorted.takeRight(K).map(value => (key, value))
        }
      }
      .groupByKey()
      .map {
        case (key, values) => {
          val topKValues = values.toList.sorted.takeRight(K).reverse
          (key, topKValues)
        }
      }
    println(s"分区topN实现方式二结果：\n" + result2.collect().mkString("\n"))

    /** 分区topN实现方式三：解决groupByKey缺点1/2
      *
      * def aggregateByKey[U: ClassTag](zeroValue: U)(seqOp: (U, V) => U,
      * combOp: (U, U) => U)
      * zeroValue：初始值，每组Key对应的初始值，即最初聚合值
      *
      * seqOp：对每组Key对应的value数据进行迭代处理，将Value和之前的聚合值进行聚合操作，并返回新的聚合值；
      * U就是聚合值的对象类型，V就是value的数据类型
      * =》该函数在每个分区数据聚合的时候被调用，也就是在shuffle传输数据之前
      * =》该方法属于shuffle的一个阶段，如果内存不足的时候就会保存到磁盘，所以一定不会出现OOM
      *
      * combOp：对两个分区的聚合值进行聚合操作，返回一个新的聚合值
      * =》在shuffle发送数据之后，对多个分区的返回结果进行聚合的时候触发调用
      * =》用于多个分区数据的合并；在API调用过程中内存不足的情况下，有可能在shuffle之前触发
      *
      */
    import scala.collection.mutable
    val result3: RDD[(String, List[Int])] = mapredRDD.aggregateByKey(mutable.ArrayBuffer[Int]())(
      (u, v) => {
        // 对当前Key的value数据v进行聚合操作=》将v合并到u中
        // 业务要求：获取当前Key后对应topK 的value值，也就是u中保存的是最大的K个value值
        // u：当前Key对应的已有上一个操作的聚合值（上一个操作后的结果或者初始值）
        // v：当前Key对应的一个value值
        u += v
        u.sorted.takeRight(K)
      },
      (u1, u2) => {
        // 对任意两个局部的聚合值进行聚合操作，可能发生在combiner阶段和shuffle后的最终数据聚合姐段
        u1 ++= u2
        u1.sorted.takeRight(K)
      }
    ).map(t => (t._1, t._2.toList.reverse))
    println(s"分区topN实现方式三结果：\n" + result3.collect().mkString("\n"))


    /** 分区topN实现方式四：使用“二次排序”来实现
      * 二次排序：在数据处理过程中，同组数据中，按照value进行数据排序，同组数据中，value有序
      * 隐含内容：
      * 1，同组数据在同一个分区
      * 2，数据先按照Key进行排序，然后再按照value进行排序
      * 3，数据是有序的
      * 4，一个分区中数据也是有序的
      * 5，再通过一个分区中，key 相同的数据在一起
      *
      * 基于一个自定义的数据分区器可以完成
      * 数据分区器功能：决定数据再shuffle操作后再哪个分区中，Map Reduce中Partitioner一样，spark默认分区器HashPartitioner
      *
      */
    val partitoner = new GroupSortedPartitoner(2)
    var ordering = implicitly[scala.math.Ordering[(String, Int)]]
    val tmpRDD = mapredRDD.map(t => (t, None))
    val tmpRDD2 = new ShuffledRDD[(String, Int), Option[_], Option[_]](tmpRDD, partitoner)
      .setKeyOrdering(ordering.reverse)

    val result4: RDD[(String, Int)] = tmpRDD2.mapPartitions(iter => {

      // 当前分区的数据统计，一组数据一定再一个分区中并且数据是连续的
      // 如果Key数量太多，就可能出现内存溢出的问题
      import scala.collection.mutable
      iter.foldLeft((PreValueMode("", 0), mutable.ArrayBuffer[(String, Int)]()))((a, b) => {

        val preValueMode = a._1
        val buffer = a._2

        val currentKey = b._1._1
        val currentCount = b._1._2

        if (preValueMode.preKey == currentKey) {
          // 当前key 被统计过了
          if (preValueMode.count >= K) {
            //nothing
          } else {
            buffer += currentKey -> currentCount
            preValueMode.count += 1
          }
        } else {
          // 当前Key第一次出现
          buffer += currentKey -> currentCount
          preValueMode.count = 1
          preValueMode.preKey = currentKey
        }
        (preValueMode, buffer)
      })._2.toIterator
    })
    println(s"分区topN实现方式四结果：\n" + result4.collect().mkString("\n"))


  }
}

case class PreValueMode(var preKey: String, var count: Int)

class GroupSortedPartitoner(num: Int) extends Partitioner {

  override def numPartitions: Int = num

  lazy val proxy = new HashPartitioner(numPartitions)

  override def getPartition(key: Any): Int = {
    key match {
      case (k, v) => {
        // 根据Key决定分区
        proxy.getPartition(k)
      }
      case _ => 0
    }
  }
}
