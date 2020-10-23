package com.njbdqn.call

import com.njbdqn.datahandler.ALSDataHandler
import com.njbdqn.datahandler.ALSDataHandler.{goods_to_num, user_to_num}
import com.njbdqn.util.{HDFSConnection, MYSQLConnection}
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object ALSCall {
  def als_call(spark:SparkSession): Unit ={
    val goodstab:DataFrame = goods_to_num(spark)
    val custstab:DataFrame = user_to_num(spark)
    val alldata: RDD[Rating] = ALSDataHandler.als_data(spark).cache()
    // 将获得的Rating集合拆分按照0.2,0.8比例拆成两个集合
   // val Array(train,test) = alldata.randomSplit(Array(0.8,0.2))
    // 使用8成的数据去训练模型
    val model = new ALS().setCheckpointInterval(2).setRank(10).setIterations(20).setLambda(0.01).setImplicitPrefs(false)
      .run(alldata)
    // 对模型进行测试,每个用户推荐前30名商品
    val tj = model.recommendProductsForUsers(30)
    import spark.implicits._
    // (uid,gid,rank)
    val df5 = tj.flatMap{
      case(user:Int,ratings:Array[Rating])=>
        ratings.map{case (rat:Rating)=>(user,rat.product,rat.rating)}
    }.toDF("uid","gid","rank")
      // 还原成(cust_id,good_id,score)
      .join(goodstab,Seq("gid"),"inner")
      .join(custstab,Seq("uid"),"inner")
      .select($"cust_id",$"good_id",$"rank")
   //   .show(false)
    HDFSConnection.writeDataToHDFS("/myshops/dwd_ALS_Iter20",df5)
  }
}
