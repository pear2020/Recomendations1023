package com.njbdqn.util
import java.util.Properties

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object MYSQLConnection {

  val paramMap = ReadPropertiesFileTool.readProperties("mysql")

    // 读取数据库中指定的表
    def readMySql(spark:SparkSession,tableName:String): DataFrame ={
      val map:Map[String,String] = Map(
        "driver"->paramMap("driver"),
        "url"->paramMap("url"),
        "user"->paramMap("user"),
        "password"->paramMap("password"),
        "dbtable"->tableName
      )
      spark.read.format("jdbc").options(map) // Adds input options for the underlying data source
        .load()
    }

  // 将df写入数据库到指定的表
  def writeTable(spark:SparkSession,df:DataFrame,tableName:String): Unit ={
    val prop = new Properties()
    prop.put("user","root")
    prop.put("password","root")
    df.write.mode(SaveMode.Overwrite).jdbc("jdbc:mysql://192.168.56.111:3306/myshops2",tableName,prop)
  }

}
