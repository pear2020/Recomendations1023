package com.njbdqn.datahandler

import com.njbdqn.util.{HDFSConnection, MYSQLConnection}
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{row_number, sum, udf}

object ALSDataHandler {
  // 为了防止用户编号或商品编号中含有非数字情况，要对所有的商品和用户编号给一个连续的对应的数字编号后再存到缓存
  def goods_to_num(spark:SparkSession):DataFrame={
    import spark.implicits._
    val wnd1 = Window.orderBy("good_id")
    HDFSConnection.readDataToHDFS(spark,"/myshops/dwd_good").select("good_id","price")
      .select($"good_id",row_number().over(wnd1).alias("gid")).cache()
  }

  def user_to_num(spark:SparkSession):DataFrame={
    import spark.implicits._
    val wnd2 = Window.orderBy("cust_id")
    MYSQLConnection.readMySql(spark,"customs")
      .select($"cust_id",row_number().over(wnd2).alias("uid")).cache()
  }

  val actToNum=udf{
    (str:String)=>{
      str match {
        case "BROWSE"=>1
        case "COLLECT"=>2
        case "BUYCAR"=>3
        case _=>8
      }
    }
  }

  case class UserAction(act:String,act_time:String,cust_id:String,good_id:String,browse:String)

  def als_data(spark:SparkSession): RDD[Rating] ={
    val goodstab:DataFrame = goods_to_num(spark)
    val custstab:DataFrame = user_to_num(spark)
    val txt = spark.sparkContext.textFile("file:///D:/logs/virtualLogs/*.log").cache()
    import spark.implicits._
    // 计算出每个用户对该用户接触过的商品的评分
    val df = txt.map(line=>{
      val arr = line.split(" ")
      UserAction(arr(0),arr(1),arr(2),arr(3),arr(4))
    }).toDF().drop("act_time","browse")
      .select($"cust_id",$"good_id",actToNum($"act").alias("score"))
      .groupBy("cust_id","good_id")
      .agg(sum($"score").alias("score"))
    // 为了防止用户编号或商品编号中含有非数字情况，要对所有的商品和用户编号给一个连续的对应的数字编号后再存到缓存
    // 将df和goodstab、custtab join一下只保留 (gid,uid,score)
    val df2 = df.join(goodstab,Seq("good_id"),"inner")
      .join(custstab,Seq("cust_id"),"inner")
      .select("gid","uid","score")
    //.show(20)
    // 将稀疏表转为 Rating对象集合
    val allData:RDD[Rating] = df2.rdd.map(row=>{
      Rating(
        row.getAs("uid").toString.toInt,
        row.getAs("gid").toString.toInt,
        row.getAs("score").toString.toFloat
      )})

    allData
  }
}
