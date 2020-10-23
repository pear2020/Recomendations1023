package com.njbdqn.root

import com.njbdqn.call.{ALSCall, GlobalHotCall, GroupCall}
import com.njbdqn.datahandler.{ALSDataHandler, KMeansHandler}
import com.njbdqn.filter.BarGoodFilter
import com.njbdqn.util.{HDFSConnection, MYSQLConnection, ReadPropertiesFileTool}
import org.apache.hadoop.fs.Path
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object App {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("app").getOrCreate();
    /**
     *  删除checkpoint留下的过程数据
     */
    val path = new Path(HDFSConnection.paramMap("hadoop_url")+"/checkpoint"); //声明要操作（删除）的hdfs 文件路径
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    if(hdfs.exists(path)) {
      //需要递归删除设置true，不需要则设置false
      hdfs.delete(path, true) //这里因为是过程数据，可以递归删除
    }

    /**
     * 设置 CheckpointDir
     */
    spark.sparkContext.setCheckpointDir(HDFSConnection.paramMap("hadoop_url")+"/checkpoint")

    // 调用过滤模块先过滤不能推荐的商品
   // BarGoodFilter.ban(spark)
    // 调用召回模块完成任务
    // 1.全局热卖召回
   //   GlobalHotCall.hotSell(spark)
    // 2.分组
  //KMeansHandler.user_group(spark)
   // GroupCall.calc_groups(spark)

    // 3.ALS
    ALSCall.als_call(spark)
    // 调用LRDataHandler并使用NotGoodFilter过滤用户
    // 调用LR来排序

    spark.stop()
  }
}
