package com.njbdqn.util

import java.io.FileInputStream
import java.util.Properties
object ReadPropertiesFileTool {

  def readProperties(flag:String): Map[String,String] ={
    val prop = new Properties()
    prop.load(new FileInputStream
      (ReadPropertiesFileTool.getClass.getClassLoader.getResource("driver.properties").getPath))
    var map:Map[String,String] = Map.empty
    if(flag.equalsIgnoreCase("mysql")){
      map+=("driver"->prop.getProperty("driver"))
      map+=("url"->prop.getProperty("url"))
      map+=("user"->prop.getProperty("user"))
      map+=("password"->prop.getProperty("password"))
    }else{
      map+=("hadoop_url"->prop.getProperty("hadoop_url"))
    }
    map
  }

}
