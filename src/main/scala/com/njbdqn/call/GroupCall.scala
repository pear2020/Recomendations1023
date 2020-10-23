package com.njbdqn.call

import com.njbdqn.datahandler.KMeansHandler
import com.njbdqn.util.{HDFSConnection, MYSQLConnection}
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


/**
 *  计算用户分组
 */
object GroupCall {
  def calc_groups(spark:SparkSession): Unit ={
    //使用Kmeans算法进行分组
    //计算根据不同的质心点计算所有的距离
    //记录不同质心点距离的集合
    //    val disList:ListBuffer[Double]=ListBuffer[Double]()
    //    for (i<-2 to 40){
    //      val kms=new KMeans().setFeaturesCol("feature").setK(i)
    //      val model=kms.fit(resdf)
    //    // 为什么不transform ??
    //      // 目的不是产生df：cust_id，feature和对应的group(prediction)
    //      // 目的是用computeCost算K数量对应的[SSD]
    //      disList.append(model.computeCost(resdf))
    //    }
    //    //调用绘图工具绘图
    //    val chart=new LineGraph("app","Kmeans质心和距离",disList)
    //    chart.pack()
    //    RefineryUtilities.centerFrameOnScreen(chart)
    //    chart.setVisible(true)

    import spark.implicits._
    val orderTable=MYSQLConnection.readMySql(spark,"orders").select("ord_id","cust_id")
    val orddetailTable=MYSQLConnection.readMySql(spark,"orderItems").select("ord_id","good_id","buy_num")
    val resdf = KMeansHandler.user_group(spark)
        //使用 Kmeans 进行分组：找一个稳定的 K 值
        val kms=new KMeans().setFeaturesCol("feature").setK(40)
        // 每个用户所属的组 (cust_id,groups) (1,0)
        val user_group_tab=kms.fit(resdf)
          .transform(resdf)
          .drop("feature").
          withColumnRenamed("prediction","groups").cache()

        //获取每组用户购买的前30名商品
        // row_number 根据组分组，买的次数desc
        // groupby 组和商品，count买的次数order_id
        val rank=30
        val wnd=Window.partitionBy("groups").orderBy(desc("group_buy_count"))

    val groups_goods = user_group_tab.join(orderTable,Seq("cust_id"),"inner")
         .join(orddetailTable,Seq("ord_id"),"inner")
          .na.fill(0)
          .groupBy("groups","good_id")
          .agg(count("ord_id").as("group_buy_count"))
          .withColumn("rank",row_number()over(wnd))
          .filter($"rank"<=rank)
        // 每个用户所属组推荐的商品（是为每个用户推荐的）
    val df5 = user_group_tab.join(groups_goods,Seq("groups"),"inner")
          HDFSConnection.writeDataToHDFS("/myshops/dwd_kMeans",df5)
  }
}
