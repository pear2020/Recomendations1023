package com.njbdqn.util

import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * HDFS操作
 */
object HDFSConnection {

  val paramMap = ReadPropertiesFileTool.readProperties("hadoop")

  /**
   * 将数据写入到hdfs
   */
  def writeDataToHDFS(path:String,df:DataFrame): Unit ={
    df.write.mode(SaveMode.Overwrite).save(paramMap("hadoop_url")+path)
  }

  /**
   * 从hdfs的指定位置读到内存中
   */
  def readDataToHDFS(spark:SparkSession,path:String): DataFrame ={
    spark.read.parquet(paramMap("hadoop_url")+path)
  }
  /**
   * 从hdfs读取LR
   */
  def readLRModelToHDFS(path:String): LogisticRegressionModel ={
    LogisticRegressionModel.read.load(paramMap("hadoop_url")+path)
  }

  /**
   *  LR模型写入HDFS
   */
  def writeLRModelToHDFS(lr:LogisticRegression,path:String): Unit ={
    lr.save(paramMap("hadoop_url")+path)
  }

}
