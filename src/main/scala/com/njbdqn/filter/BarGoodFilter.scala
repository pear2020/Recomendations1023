package com.njbdqn.filter

import com.njbdqn.util.{HDFSConnection, MYSQLConnection}
import org.apache.spark.sql.SparkSession

object BarGoodFilter {

  /**
   *  清洗不能推荐的商品，把留下的商品存在HDFS上保存
   * @param spark
   */
  def ban(spark:SparkSession): Unit ={
   // 读出原始数据
   val goodsDf = MYSQLConnection.readMySql(spark, "goods")
    // 下架商品(已经卖过),存放到HDFS
    val gd = goodsDf.filter("is_sale=1")
    HDFSConnection.writeDataToHDFS("/myshops/dwd_good",gd)
  }
}
