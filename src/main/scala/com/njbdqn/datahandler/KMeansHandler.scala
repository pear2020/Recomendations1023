package com.njbdqn.datahandler

import com.njbdqn.util.{HDFSConnection, MYSQLConnection}
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.{MinMaxScaler, StringIndexer, VectorAssembler}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, count, current_date, datediff, desc, min, row_number, sum, udf}
import org.apache.spark.sql.types.DoubleType

object KMeansHandler {
  val func_membership = udf {
    (score: Int) => {
      score match {
        case i if i < 100 => 1
        case i if i < 500 => 2
        case i if i < 1000 => 3
        case _ => 4
      }
    }
  }

  val func_bir = udf {
    (idno: String, now: String) => {
      val year = idno.substring(6, 10).toInt
      val month = idno.substring(10, 12).toInt
      val day = idno.substring(12, 14).toInt

      val dts = now.split("-")
      val nowYear = dts(0).toInt
      val nowMonth = dts(1).toInt
      val nowDay = dts(2).toInt

      if (nowMonth > month) {
        nowYear - year
      } else if (nowMonth < month) {
        nowYear - 1 - year
      } else {
        if (nowDay >= day) {
          nowYear - year
        } else {
          nowYear - 1 - year
        }
      }
    }
  }

  val func_age = udf {
    (num: Int) => {
      num match {
        case n if n < 10 => 1
        case n if n < 18 => 2
        case n if n < 23 => 3
        case n if n < 35 => 4
        case n if n < 50 => 5
        case n if n < 70 => 6
        case _ => 7
      }
    }
  }

  val func_userscore = udf {
    (sc: Int) => {
      sc match {
        case s if s < 100 => 1
        case s if s < 500 => 2
        case _ => 3
      }
    }
  }

  val func_logincount = udf {
    (sc: Int) => {
      sc match {
        case s if s < 500 => 1
        case _ => 2
      }
    }
  }
  def user_group(spark:SparkSession) = {
    val featureDataTable = MYSQLConnection.readMySql(spark,"customs").filter("active!=0")
      .select("cust_id", "company", "province_id", "city_id", "district_id"
        , "membership_level", "create_at", "last_login_time", "idno", "biz_point", "sex", "marital_status", "education_id"
        , "login_count", "vocation", "post")
    //商品表
    val goodTable=HDFSConnection.readDataToHDFS(spark,"/myshops/dwd_good").select("good_id","price")
    //订单表
    val orderTable=MYSQLConnection.readMySql(spark,"orders").select("ord_id","cust_id")
    //订单明细表
    val orddetailTable=MYSQLConnection.readMySql(spark,"orderItems").select("ord_id","good_id","buy_num")
    //先将公司名通过StringIndex转为数字
    val compIndex = new StringIndexer().setInputCol("company").setOutputCol("compId")
    //使用自定义UDF函数
    import spark.implicits._
    //计算每个用户购买的次数
    val tmp_bc=orderTable.groupBy("cust_id").agg(count($"ord_id").as("buycount"))
    //计算每个用户在网站上花费了多少钱
    val tmp_pay=orderTable.join(orddetailTable,Seq("ord_id"),"inner").join(goodTable,Seq("good_id"),"inner").groupBy("cust_id").
      agg(sum($"buy_num"*$"price").as("pay"))

    val df=compIndex.fit(featureDataTable).transform(featureDataTable)
      .withColumn("mslevel", func_membership($"membership_level"))
      .withColumn("min_reg_date", min($"create_at") over())
      .withColumn("reg_date", datediff($"create_at", $"min_reg_date"))
      .withColumn("min_login_time", min("last_login_time") over())
      .withColumn("lasttime", datediff($"last_login_time", $"min_login_time"))
      .withColumn("age", func_age(func_bir($"idno", current_date())))
      .withColumn("user_score", func_userscore($"biz_point"))
      .withColumn("logincount", func_logincount($"login_count"))
      // 右表：有的用户可能没有买/没花钱，缺少cust_id，所以是left join，以多的为准
      .join(tmp_bc,Seq("cust_id"),"left").join(tmp_pay,Seq("cust_id"),"left")
      .na.fill(0)
      .drop("company", "membership_level", "create_at", "min_reg_date"
        , "last_login_time", "min_login_time", "idno", "biz_point", "login_count")
    //将所有列换成 Double
    val columns=df.columns.map(f=>col(f).cast(DoubleType))
    val num_fmt=df.select(columns:_*)
    //将除了第一列的所有列都组装成一个向量列
    val va= new VectorAssembler()
      .setInputCols(Array("province_id","city_id","district_id","sex","marital_status","education_id","vocation","post","compId","mslevel","reg_date","lasttime","age","user_score","logincount","buycount","pay"))
      .setOutputCol("orign_feature")
    val ofdf=va.transform(num_fmt).select("cust_id","orign_feature")
    //将原始特征列归一化处理
    val mmScaler:MinMaxScaler=new MinMaxScaler().setInputCol("orign_feature").setOutputCol("feature")
    //fit产生模型 把ofdf放到模型里使用

      mmScaler.fit(ofdf)
      .transform(ofdf)
      .select("cust_id","feature")

  }
}
