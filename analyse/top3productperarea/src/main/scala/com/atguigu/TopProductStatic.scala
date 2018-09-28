package com.atguigu


import java.util.UUID

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.model.UserVisitAction
import commons.utils.ParamUtils
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object TopProductStatic {

  def main(args: Array[String]): Unit = {
    val taskJsonStr = ConfigurationManager.config.getString(Constants.TASK_PARAMS)
    val taskJsonObj = JSONObject.fromObject(taskJsonStr)
    //创建全局唯一的主键
    val taskUUID = UUID.randomUUID().toString
    val conf = new SparkConf().setAppName("topprodectstatic").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    val clickActionRDD = getProductCityInfo(spark,taskJsonObj)
    val cityAreaInfoRDD = getAreaInfo(spark)

    val basicInfoRDD = clickActionRDD.join(cityAreaInfoRDD).map{
      case (cityId,(clickActionInfo,cityAreaInfo))=>
        (cityId,cityAreaInfo.city_name,cityAreaInfo.area,clickActionInfo.click_product_id)
    }
    import spark.implicits._
    basicInfoRDD.toDF("city_id", "city_name", "area", "product_id").createOrReplaceTempView("tmp_basic_info_table")

    spark.udf.register("cityIdAndNameConcat",(s1:String,s2:String,separator:String)=>{
      s1+separator+s2
    })

    spark.udf.register("cityIdNameAgg",new CityIdNameAggregateFunction)

    val sql = "select area,product_id,count(*) count,cityIdNameAgg(cityIdAndNameConcat(city_id,city_name,':')) city_Infos from " +
      "tmp_basic_info_table group by area,product_id"
    spark.sql(sql).createOrReplaceTempView("tmp_area_click_count_table")

    getCityProductInfo(spark)

    getTop3Product(spark)
  }

  def getProductCityInfo(spark: SparkSession,taskJsonObj:JSONObject)={
    val startTime = ParamUtils.getParam(taskJsonObj,Constants.PARAM_START_DATE)
    val endTime = ParamUtils.getParam(taskJsonObj,Constants.PARAM_END_DATE)
    val sql = s"select city_id,click_product_id from user_visit_action where date>='${startTime}' and date <='${endTime}' " +
              s"and click_product_id != -1"
    import spark.implicits._
    spark.sql(sql).as[CityClickProduct].rdd.map{
      case clickProduct=>(clickProduct.city_id,clickProduct)
    }
  }

  def getAreaInfo(sparkSession: SparkSession) = {
    val areaInfo = Array((0L, "北京", "华北"), (1L, "上海", "华东"), (2L, "南京", "华东"),
      (3L, "广州", "华南"), (4L, "三亚", "华南"), (5L, "武汉", "华中"),
      (6L, "长沙", "华中"), (7L, "西安", "西北"), (8L, "成都", "西南"),
      (9L, "哈尔滨", "东北"))
    sparkSession.sparkContext.makeRDD(areaInfo).map{
      case (cityId,cityName,area) =>
        val cityAreaObjRDD = CityAreaInfo(cityId,cityName,area)
        (cityId,cityAreaObjRDD)
    }
  }

  def getCityProductInfo(spark: SparkSession) = {
    spark.udf.register("getExtendInfo",(jsonStr:String,fieldStr:String)=>{
      val jsonObj = JSONObject.fromObject(jsonStr)
      jsonObj.getString(fieldStr)
    })

    val sql = "select area,city_infos,acct.product_id,product_name," +
      "if(getExtendInfo(extend_info,'product_status')='0','Self','Third Party') product_status,count from " +
      "tmp_area_click_count_table acct join product_info pi on acct.product_id=pi.product_id"
    spark.sql(sql).createOrReplaceTempView("tmp_area_count_full_info_table")
  }

  def getTop3Product(spark: SparkSession)= {
    val sql = "select area," +
      "case " +
      "when area='华北' or area='华东' then 'A_Level' " +
      "when area='华南' or area='华中' then 'B_Level'" +
      "when area='西北' or area='西南' then 'C_Level'" +
      "when area='东北' or area='其他' then 'D_Level'" +
      "end area_level," +
      "product_id,city_infos,count,product_name,product_status " +
      "from (select area,product_id,city_infos,count,product_name,product_status,row_number() " +
      "over(partition by area order by count desc) rank from tmp_area_count_full_info_table) temp where rank<=3"

    val areaTop3ProductRDD = spark.sql(sql).rdd.map{
      val taskId = UUID.randomUUID().toString
      row =>
        AreaTop3Product(taskId,row.getAs[String]("area"),row.getAs[String]("area_level"),row.getAs[Long]("product_id"),
          row.getAs[String]("city_infos"),row.getAs[Long]("count"),
          row.getAs[String]("product_name"),row.getAs[String]("product_status"))
    }
    import spark.implicits._
    val mm = areaTop3ProductRDD.toDS()
    .write
    .format("jdbc")
    .option("url",ConfigurationManager.config.getString(Constants.JDBC_URL))
    .option("user",ConfigurationManager.config.getString(Constants.JDBC_USER))
    .option("password",ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
    .option("dbtable","areatop3_product_0416")
    .mode(SaveMode.Append)
    .save()
  }
}
