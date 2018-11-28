package com.lxgy.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * @author Gryant
  */
object DataFrame2RDD {

  def main(args: Array[String]): Unit = {


    val warehouseLocation = "/user/hive/warehouse"

    val spark = SparkSession
      .builder()
      .appName("DataFrame2RDD")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    /*val deptDF: DataFrame = spark.read
      .table("common.dept")
    deptDF.show()

    // df -> rdd
    val deptRDD: RDD[Row] = deptDF.rdd
    deptRDD.foreachPartition(iter => iter.foreach(println))*/


    // rdd -> df
    /*
    val rdd: RDD[(Int, String, Double)] = spark.sparkContext.parallelize(Array(
      (1,"lxg",99.2),
      (2,"gy",98.1),
      (3,"yfx",95.5),
      (4,"ybz",96.5)
    ))

    // 使用默认的case class对象的属性名称作为列名称
    val df1: DataFrame = rdd.toDF()
    df1.show()

    // 给定具体额列名称，列名称必须一直
    val df2 = rdd.toDF("id","name","grade")
    df2.show()


    // 明确给定schema信息转换
    val rowRDD: RDD[Row] = rdd.map{
      case (id,name,grade)=>{
        Row(id,name,grade)
      }
    }

    val schema = StructType(Array(
      StructField("id",IntegerType),
      StructField("name",StringType),
      StructField("grade",DoubleType)
    ))

    val df3 = spark.createDataFrame(rowRDD, schema)
    df3.show()*/

    // rdd -> ds
    val rdd: RDD[(String, Int)] = spark.sparkContext.parallelize(Array(
      ("lxg", 99),
      ("gy", 98),
      ("yfx", 95),
      ("ybz", 96)
    ))

    // 利用反射机制
    val ds1: Dataset[(String, Int)] = rdd.toDS()
    ds1.show()

    ds1.map(per=>{
      Person(per._1,per._2)
    }).show()

    // 转为df
    val df1: DataFrame = rdd.toDF("name","grade")
    val ds2 = df1.as[Person]
    ds2.show()


  }

}

case class Person(name:String,grade:Int)
