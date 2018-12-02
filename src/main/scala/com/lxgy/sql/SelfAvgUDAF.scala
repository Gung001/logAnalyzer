package com.lxgy.sql

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

/**
  * 自定义求平均值UDAF函数
  *
  * @author Gryant
  */
object SelfAvgUDAF extends UserDefinedAggregateFunction {

  override def inputSchema: StructType = {
    // 给定输入数据的类型
    StructType(Array(
      StructField("v1", DoubleType)
    ))
  }

  override def bufferSchema: StructType = {
    // 指定缓存数据的数据类型
    StructType(Array(
      // 缓存总和
      StructField("b1", DoubleType),
      // 缓存的总数据量
      StructField("b2", IntegerType)
    ))
  }

  override def dataType: DataType = {

    // 指定UDAF返回数据类型
    DoubleType

  }

  override def deterministic: Boolean = {
    // 指定多次执行的时候，返回结果是否必须一致
    true
  }

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    // 初始化缓存
    buffer.update(0, 0.0) // 初始化总和
    buffer.update(1, 0) // 初始化总数量
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    // 对于每一条输入的input数据进行操作
    // 读取输入数据
    val inputValue = input.getDouble(0)

    // 读取缓存数据
    val bufferSum = buffer.getDouble(0)
    val bufferCount = buffer.getInt(1)

    // 更新缓存区的值
    buffer.update(0, bufferSum + inputValue)
    buffer.update(1, bufferCount + 1)
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {

    // 只有当多个存在的时候才会调用此方法，将buffer2中的结果添加到buffer1中

    // 获取buffer1中的数据
    val buffer1Sum = buffer1.getDouble(0)
    val buffer1Count = buffer1.getInt(1)

    // 获取buffer2中的数据
    val buffer2Sum = buffer2.getDouble(0)
    val buffer2Count = buffer2.getInt(1)

    // 更新buffer1中的数
    buffer1.update(0, buffer1Sum + buffer2Sum)
    buffer1.update(1, buffer1Count + buffer2Count)
  }

  override def evaluate(buffer: Row): Any = {

    // 计算最终结果
    buffer.getDouble(0) / buffer.getInt(1)
  }
}
