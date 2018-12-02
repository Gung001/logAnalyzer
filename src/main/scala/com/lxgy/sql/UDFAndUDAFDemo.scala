package com.lxgy.sql

import org.apache.spark.sql.SparkSession

/**
  * @author Gryant
  */
object UDFAndUDAFDemo {

  def main(args: Array[String]): Unit = {

    val warehouseLocation = "/user/hive/warehouse"

    val spark = SparkSession
      .builder()
      .appName("UDFAndUDAFDemo")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .master("local[*]")
      .getOrCreate()

    // 自定义udf 函数
    spark.sqlContext.udf.register("strLen", (s: String) => s.length)
    spark.sqlContext.udf.register("format_double", (s: String,scale:Int) => {
      import java.math.BigDecimal
      new BigDecimal(s).setScale(scale, BigDecimal.ROUND_HALF_UP).stripTrailingZeros().toPlainString
    })

    // 自定义udaf 函数
    spark.sqlContext.udf.register("self_avg", SelfAvgUDAF)

    // 自定义udaf 函数

    val empDF = spark.read.table("common.emp")
    //    empDF.printSchema()
    //    empDF.show(false)
    empDF.select("deptno", "sal").createOrReplaceTempView("tmp_emp")

    spark.sql(
      """
        |select deptno,strLen(deptno) as el,self_avg(sal) selfAvgSal,AVG(sal) sal,format_double(AVG(sal),1) formatSal
        |from tmp_emp
        |group by deptno
      """.stripMargin)
      .show(false)

  }

}
