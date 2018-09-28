package com.atguigu

import java.util.UUID

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.model.{PageSplitConvertRate, UserVisitAction}
import commons.utils.{DateUtils, ParamUtils, StringUtils}
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object PageStat {

  def main(args: Array[String]): Unit = {
    val jsonStr = ConfigurationManager.config.getString(Constants.TASK_PARAMS)
    val jsonObj = JSONObject.fromObject(jsonStr)
    val conf = new SparkConf().setAppName("pagestat").setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    val taskId = UUID.randomUUID().toString
    //pageFlowArray:Array[1,2,...,7]
    val pageFlowArray = ParamUtils.getParam(jsonObj,Constants.PARAM_TARGET_PAGE_FLOW).split(",")
    val pageFlowWithoutTail = pageFlowArray.slice(0,pageFlowArray.length-1)
    //pageFlowZip:Array[(String,String)]    [(1,2),(2,3)....(5,6),(6,7)]
    val pageFlowZip = pageFlowWithoutTail.zip(pageFlowArray.tail)
    //[1_2,2_3,3_4,...6_7]
    val pageFlow = pageFlowZip.map{case (page1,page2)=>page1+"_"+page2}

    val sessionId2Action = getOriData(sparkSession,jsonObj)
    val page2NumRDD = sessionId2Action.groupByKey().flatMap{
      case (sessionId,actions)=>
        val actionList = actions.toList.sortWith((action1,action2)=>
          DateUtils.parseTime(action1.action_time).getTime<DateUtils.parseTime(action2.action_time).getTime)
        val pageList = actionList.map(action=>action.page_id)
        val pageSkipList = pageList.slice(0,pageList.length-1).zip(pageList.tail).map{
          case (page1,page2)=> page1+"_"+page2
        }
        val pageSkipDis = pageSkipList.filter{
          pageSkip => pageFlow.contains(pageSkip)
        }
        pageSkipDis.map(pageSkip=>(pageSkip,1L))
    }
    val pageMap = page2NumRDD.countByKey()

    //计算任务中起始首页的页面id
    val firstPage = pageFlowArray(0).toLong
    //计算数据中起始首页的个数
    val firstPageCount = sessionId2Action.filter{
      case (sessionId,action)=>
        action.page_id == firstPage
    }.count()

    getPageSkipRatio(sparkSession,pageMap,taskId,pageFlow,firstPageCount)
  }
  def getOriData(sparkSession: SparkSession,jsonObj: JSONObject) = {
    val starTime = ParamUtils.getParam(jsonObj,Constants.PARAM_START_DATE)
    val endTime = ParamUtils.getParam(jsonObj,Constants.PARAM_END_DATE)
    val sql = s"select * from user_visit_action where date>='${starTime}' and date<='${endTime}'"
    import sparkSession.implicits._
    sparkSession.sql(sql).as[UserVisitAction].rdd.map(item=>(item.session_id,item))
  }


  def getPageSkipRatio(sparkSession: SparkSession,
                       pageMap: collection.Map[String, Long],
                       taskId: String, pageFlow: Array[String],
                       firstPageCount:Long)={
    var pageRatioStr = ""
    var lastPageSkipCount = firstPageCount.toDouble
    for(pageSkip<-pageFlow){
      val currentPageSkip = pageMap.get(pageSkip).get.toLong
      val pageRatio = currentPageSkip/lastPageSkipCount
      lastPageSkipCount = currentPageSkip.toDouble
      pageRatioStr += pageSkip + "=" +pageRatio +"|"
    }
    pageRatioStr = pageRatioStr.substring(0,pageRatioStr.length-1)
    val pageSkipRatio = PageSplitConvertRate(taskId,pageRatioStr)
    val pageSkipRatioRDD = sparkSession.sparkContext.makeRDD(Array(pageSkipRatio))
    import sparkSession.implicits._
    pageSkipRatioRDD.toDF()
      .write
      .format("jdbc")
      .option("url",ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user",ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password",ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable","pageskip_ratio_0416")
      .mode(SaveMode.Append)
      .save()

  }
}
