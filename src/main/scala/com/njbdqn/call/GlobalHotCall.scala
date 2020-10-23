package com.njbdqn.call

import com.njbdqn.util.{HDFSConnection, MYSQLConnection}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{desc, row_number, sum}

/**
 *  全局召回
 */
object GlobalHotCall {

  def hotSell(spark:SparkSession): Unit ={
    val oitab = MYSQLConnection.readMySql(spark, "orderItems").cache()
    // 计算全局热卖商品前100名 ( good_id,sellnum )
    import spark.implicits._
    val top30 = oitab
      .groupBy("good_id")
      .agg(sum("buy_num").alias("sellnum"))
      .withColumn("rank",row_number().over(Window.orderBy(desc("sellnum"))))
      .limit(100)
    // 所有用户id和推荐前30名cross join
    val wnd2 = Window.orderBy("cust_id")
    val custstab = MYSQLConnection.readMySql(spark,"customs")
      .select($"cust_id").cache()
    val res = custstab.crossJoin(top30)
      .select($"cust_id",$"good_id",$"rank")
     // .rdd.saveAsTextFile("hdfs://192.168.56.111:9000/globalHotSell")
    HDFSConnection.writeDataToHDFS("/myshops/dwd_hotsell",res)
  }
}
