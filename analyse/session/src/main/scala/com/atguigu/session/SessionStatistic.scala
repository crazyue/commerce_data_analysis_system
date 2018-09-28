package com.atguigu.session

import java.util.UUID

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.model._
import commons.utils._
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random




object SessionStatistic {


  def main(args: Array[String]): Unit = {
    val taskJsonStr = ConfigurationManager.config.getString(Constants.TASK_PARAMS)
    val taskJsonObj = JSONObject.fromObject(taskJsonStr)
    //创建全局唯一的主键
    val taskUUID = UUID.randomUUID().toString
    val conf = new SparkConf().setAppName("sessionstatic").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    //按照时间限制因素读取表user_visit_action中数据，并将其转换成rdd
    val oriActionRdd = getOriActionRdd(spark,taskJsonObj)
    //oriActionRdd.foreach(println(_))
    //user_visit_action表与user_info表进行连接，提取指定字段并将其转换成rdd
    val fullInfoRdd = oriRdd2JoinRdd(oriActionRdd,spark)
    //创建自定义累加器并注册
    val acc = new SessionAccumulator
    spark.sparkContext.register(acc)
    //fullInfoRdd.foreach(println(_))
    //按照过滤条件对fullInfoRdd进行过滤，并利用自定义累加器统计对session步长和访问时长进行累加。sessionUserRdd类型RDD[(String,String)]
    val sessionUserRdd = getResultByFilter(fullInfoRdd,taskJsonObj,acc)

    //调用下这个行动算子，要不然不计算
   sessionUserRdd.foreach(println(_))

    //计算各范围session步长、访问时长占总session的比值，并将最终结果写入mysql数据库中
    writeRate2DB(spark,acc.value,taskUUID)

    //需求二
    // 根据每个小时的session数量在一天中总session数量中的占比随机抽取100条session,并将其保存到mysql数据库中
    extractSession(spark,sessionUserRdd)

    //需求三
    //sessionId2Action类型为RDD[(sessionId,UserVisitAction)]
    val sessionId2Action =oriActionRdd.map(item=>(item.session_id, item))
    //sessionUserRdd已经是按照过滤条件过滤后的数据，利用它与sessionId2Action进行join，然后map,可以使得sessionId2Action
    //也按照过滤条件进行了过滤，session2FilteredRDD为已经满足过滤条件的数据RDD[（sessionId,action）]

    val session2FilteredRDD = sessionUserRdd.join(sessionId2Action).map{case (sessionId,(fullInfo,action))=>(sessionId,action)}
    //session2FilteredRDD.foreach(println(_))
    //Array[(sortKey:SortKey, countInfo)]
    val top10CategoryArray = top10Catagory(session2FilteredRDD,spark)

    //需求四----Top10热门品类的Top10活跃Session统计
    topSessionOfCatagory(spark,session2FilteredRDD,top10CategoryArray)


  }

  def topSessionOfCatagory(spark: SparkSession, session2FilteredRDD: RDD[(String, UserVisitAction)], top10CategoryArray: Array[(SortKey, String)])={
    val cidRDD = top10CategoryArray.map{
      case (sortkey, countInfo)=>
        val cid = StringUtils.getFieldFromConcatString(countInfo,"\\|",Constants.FIELD_CATEGORY_ID).toLong
        cid
    }
    //RDD[(sessionId,action)]
    val sessionId2TopCidActionRDD = session2FilteredRDD.filter{
      case (sessionId,action)=> cidRDD.contains(action.click_category_id)
    }
      val cid2SessionCountRDD = sessionId2TopCidActionRDD.groupByKey().flatMap{
      case (sessionId,actions)=>
        val cidMap = new mutable.HashMap[Long,Long]()
        for(action<-actions){
          val cid = action.click_category_id
          if(!cidMap.contains(cid)){
            cidMap += (cid->0)
            cidMap(cid) = cidMap(cid)+1
          }
        }
        for((cid,count)<-cidMap)
          yield(cid,sessionId+"="+count)  //不能添加{ }
    }

    val cid2GroupRDD = cid2SessionCountRDD.groupByKey()

    val cid2SessionTop10RDD = cid2GroupRDD.flatMap{
      case (cid,sessionCounts)=>
        val sessionList = sessionCounts.toList.sortWith((member1,member2)=>member1.split("=")(1).toLong
          > member2.split("=")(1).toLong).take(10)
        val arrayBuffer = new mutable.ArrayBuffer[Top10Session]()
      for(sessionIdCountStr <-sessionList){
        val taskId = UUID.randomUUID().toString
        val sessionId = sessionIdCountStr.split("=")(0)
        val clickCount = sessionIdCountStr.split("=")(1).toLong
        val top10Session = Top10Session(taskId,cid,sessionId,clickCount)
        arrayBuffer.append(top10Session)
      }
        arrayBuffer
    }
    cid2SessionTop10RDD.foreach(println(_))
    import spark.implicits._
    cid2SessionTop10RDD.toDF().write.format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable","top10session_percategories_0416")
      .mode(SaveMode.Append)
      .save()

  }

  def getClickCount(session2FilteredRDD: RDD[(String, UserVisitAction)]) = {
    val clickFilterRDD = session2FilteredRDD.filter{case (sessionId,action)=>
      action.click_category_id != -1L
    }
    val cid2NumRDD = clickFilterRDD.map{case (sessionId,action)=>
      (action.click_category_id,1L)
    }
    cid2NumRDD.reduceByKey(_+_)
  }

  def getOrderCount(session2FilteredRDD: RDD[(String, UserVisitAction)]) = {
    val orderFilterRDD = session2FilteredRDD.filter{ case(sessionId,action) =>
      action.order_category_ids != null
    }
    val cid2NumRDD = orderFilterRDD.flatMap{case (sessionId,action)=>
        action.order_category_ids.split(",").map(item=>(item.toLong,1L))
    }
    cid2NumRDD.reduceByKey(_+_)
  }

  def getPayCount(session2FilteredRDD: RDD[(String, UserVisitAction)]) = {
    val orderFilterRDD = session2FilteredRDD.filter{ case(sessionId,action) =>
      action.pay_category_ids != null
    }
    val cid2NumRDD = orderFilterRDD.flatMap{case (sessionId,action)=>
      action.pay_category_ids.split(",").map(item=>(item.toLong,1L))
    }
    cid2NumRDD.reduceByKey(_+_)
  }

  def getCidCountInfo(cid2CidDfRDD: RDD[(Long, Long)],
                      cid2ClickCountRDD: RDD[(Long, Long)],
                      cid2OrderCountRDD: RDD[(Long, Long)],
                      cid2PayCountRDD: RDD[(Long, Long)]) = {
    val cid2ClickInfoRDD = cid2CidDfRDD.leftOuterJoin(cid2ClickCountRDD).map{case (cid,(categoryId,option))=>
      val count = if(option.isDefined) option.get else 0
      val clickCountInfo = Constants.FIELD_CATEGORY_ID + "=" + cid +"|" +
        Constants.FIELD_CLICK_COUNT + "=" + count
      (cid,clickCountInfo)
    }
   val cid2OrderInfoRDD = cid2ClickInfoRDD.leftOuterJoin(cid2OrderCountRDD).map{case (cid,(clickCountInfo,option))=>
      val count = if(option.isDefined) option.get else 0
      val orderCountInfo = clickCountInfo + "|" + Constants.FIELD_ORDER_COUNT + "=" + count
      (cid,orderCountInfo)
    }
    val cid2PayInfoRDD = cid2OrderInfoRDD.leftOuterJoin(cid2PayCountRDD).map{case (cid,(orderCountInfo,option))=>
      val count = if(option.isDefined) option.get else 0
      val payCountInfo = orderCountInfo + "|" + Constants.FIELD_PAY_COUNT+ "=" + count
      (cid,payCountInfo)
    }
    cid2PayInfoRDD
  }

  def top10Catagory(session2FilteredRDD: RDD[(String, UserVisitAction)], spark: SparkSession)= {
    //session2FilteredRDD类型RDD[(String, UserVisitAction)]

    val cid2CidRDD = session2FilteredRDD.flatMap{case (sessionId,action)=>
      val arrayBuffer = new mutable.ArrayBuffer[(Long,Long)]()
      val click_id = action.click_category_id
      val order_ids = action.order_category_ids
      val pay_ids = action.pay_category_ids
      if(click_id != -1)
        arrayBuffer.append((click_id,click_id))

      if(order_ids != null)
        for(order_id <- order_ids.split(","))
        arrayBuffer.append((order_id.toLong,order_id.toLong))

      if(pay_ids != null)
        for(pay_id <- pay_ids.split(","))
        arrayBuffer.append((pay_id.toLong,pay_id.toLong))
      arrayBuffer
    }
    val cid2CidDfRDD = cid2CidRDD.distinct()

    val cid2ClickCountRDD = getClickCount(session2FilteredRDD)

    val cid2OrderCountRDD = getOrderCount(session2FilteredRDD)

    val cid2PayCountRDD = getPayCount(session2FilteredRDD)

    val cid2CountInfoRDD = getCidCountInfo(cid2CidDfRDD,cid2ClickCountRDD,cid2OrderCountRDD,cid2PayCountRDD)

    val sortKey2CountInfoRDD = cid2CountInfoRDD.map{case (cid,catagoryInfo)=>
      val clickCount = StringUtils.getFieldFromConcatString(catagoryInfo,"\\|",Constants.FIELD_CLICK_COUNT).toLong
      val orderCount = StringUtils.getFieldFromConcatString(catagoryInfo,"\\|",Constants.FIELD_ORDER_COUNT).toLong
      val payCount = StringUtils.getFieldFromConcatString(catagoryInfo,"\\|",Constants.FIELD_PAY_COUNT).toLong
      val sortKey = new SortKey(clickCount,orderCount,payCount)
      (sortKey,catagoryInfo)
    }
    // take是行动算子，其返回值为Array类型。Array[(sortKey, countInfo)]
    val top10CategoryArray = sortKey2CountInfoRDD.sortByKey(false).take(10)
    val top10CategoryRDD = spark.sparkContext.makeRDD(top10CategoryArray).map{case(sortKey,countInfo)=>
      val categoryId = StringUtils.getFieldFromConcatString(countInfo,"\\|",Constants.FIELD_CATEGORY_ID).toLong
      val taskId = UUID.randomUUID().toString
      Top10Category(taskId,categoryId,sortKey.clickCount,sortKey.orderCount,sortKey.payCount)
    }

    import spark.implicits._
    top10CategoryRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable","top10_categories_0416")
      .mode(SaveMode.Append)
      .save()

    top10CategoryArray
  }


  def generateHourExtractList(hour2List: mutable.HashMap[String, ListBuffer[Int]], countPerDay: Int, totalCountPerDay: Long, hourCountMap: mutable.HashMap[String, Long]) = {
    //countPerDay一天要抽多少条session
    //totalCountPerDay一天有多少条session
    val random = new Random()
    for((hour,count)<-hourCountMap){
      var countPerHour = (count.toDouble/totalCountPerDay*countPerDay).toLong
      if(countPerHour>count)
        countPerHour = count

      hour2List.get(hour) match{
        case None => hour2List(hour) = new ListBuffer[Int]
          for(num<- 0 until countPerHour.toInt){
            var randomNum = random.nextInt(count.toInt)
            while(hour2List(hour).contains(randomNum)){
              randomNum = random.nextInt(count.toInt)
            }
            hour2List(hour).append(randomNum)
          }
          //我觉得这里不需要再判断Some(list)的情况
        case Some(list) =>
          for(num<- 0 until countPerHour.toInt){
            var randomNum = random.nextInt(count.toInt)
            while(hour2List(hour).contains(randomNum)){
              randomNum = random.nextInt(count.toInt)
            }
            hour2List(hour).append(randomNum)
          }
      }
    }
  }

  def extractSession(spark: SparkSession, sessionUserRdd: RDD[(String, String)])={
    val dateHourFullInfoRdd = sessionUserRdd.map{case (sessionId,sessionInfo)=>
      val time = StringUtils.getFieldFromConcatString(sessionInfo,"\\|",Constants.FIELD_START_TIME)
      //yyyy-MM-dd_HH
      val datehour = DateUtils.getDateHour(time)
      (datehour,sessionInfo)
    }
    //dataHourCountMap的类型为MAP[dateHour:String,count:Long]
    val dataHourCountMap = dateHourFullInfoRdd.countByKey()

    val dateCountMap = new mutable.HashMap[String,mutable.HashMap[String,Long]]
    for((datehour,count)<-dataHourCountMap){
      val date = datehour.split("_")(0)
      val hour = datehour.split("_")(1)

      dateCountMap.get(date) match {
        case None => dateCountMap(date) = new mutable.HashMap[String,Long]()
          dateCountMap(date) += (hour->count)
        case Some(map) => dateCountMap(date) += (hour->count)
      }
    }
    //一天需要统计的数量
    val countPerDay = 100/dateCountMap.size
    val dateListMap = new mutable.HashMap[String,mutable.HashMap[String,ListBuffer[Int]]]
    for((date,hourCountMap)<-dateCountMap){
      //一天有多少条数据
      val totalCountPerDay = hourCountMap.values.sum
      dateListMap.get(date) match{
        case None => dateListMap(date) = new mutable.HashMap[String,ListBuffer[Int]]
          generateHourExtractList(dateListMap(date),countPerDay,totalCountPerDay,hourCountMap)
          //我觉得这里也不需要写some
        case Some(map)=>
          generateHourExtractList(dateListMap(date),countPerDay,totalCountPerDay,hourCountMap)
      }
    }

    //设定广播变量
    val dateListMapBd = spark.sparkContext.broadcast(dateListMap)


    val dateHour2FullInfosRdd = dateHourFullInfoRdd.groupByKey()
    val sessionExtractRdd = dateHour2FullInfosRdd.flatMap{
      case (dateHour, fullInfos) =>
        val objArray = new mutable.ArrayBuffer[SessionRandomExtract]()
        val date = dateHour.split("_")(0)
        val hour = dateHour.split("_")(1)
        //获取某个小时对应的list
        val indexList = dateListMapBd.value.get(date).get(hour)
        var index = 0
        for (fullInfo <- fullInfos){
          if (indexList.contains(index)) {
            val sessionId = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_SESSION_ID)
            val startTime = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_START_TIME)
            val searchKeyWords = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_SEARCH_KEYWORDS)
            val clickCategoryIds = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_CLICK_CATEGORY_IDS)
            val taskId = UUID.randomUUID().toString
            val obj = SessionRandomExtract(taskId,sessionId, startTime, searchKeyWords, clickCategoryIds)
            objArray.append(obj)
          }
          index += 1
        }
        objArray
    }
    import spark.implicits._
    sessionExtractRdd
      .toDF()
      .write
      .format("jdbc")
      .option("url",ConfigurationManager.config.getString(Constants.JDBC_URL))
      . option("user",ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password",ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable","session_extract_0416")
      .mode(SaveMode.Append)
      .save()
  }


  def getOriActionRdd(spark: SparkSession, taskJsonObj: JSONObject) = {
    val startTime = ParamUtils.getParam(taskJsonObj,Constants.PARAM_START_DATE)
    val endTime = ParamUtils.getParam(taskJsonObj,Constants.PARAM_END_DATE)
    val sql = s"select * from user_visit_action where date>='${startTime}' and date <='${endTime}'"
    import spark.implicits._
    spark.sql(sql).as[UserVisitAction].rdd
  }


  def oriRdd2JoinRdd(oriActionRdd: RDD[UserVisitAction], sparkSession:SparkSession)={
    //oriRdd2GroupRdd类型为RDD[(String, Iterable[UserVisitAction])
    val oriRdd2GroupRdd = oriActionRdd.map(item=>(item.session_id,item)).groupByKey()

    val group2AggrRdd = oriRdd2GroupRdd.map{case (session_id,items)=>
      var startTime:String = null
      var endTime:String = null
      var searchKeyWords = new StringBuffer("")
      var clickCategoryId = new StringBuffer("")
      var stepLength:Long = 0
      var userId:Long = -1

      for(item<-items){
        if(userId == -1)
          userId = item.user_id

        if(startTime==null || DateUtils.after(startTime,item.action_time))
          startTime = item.action_time

        if(endTime==null || DateUtils.before(endTime,item.action_time))
          endTime = item.action_time

        if(StringUtils.isNotEmpty(item.search_keyword) && !searchKeyWords.toString.contains(item.search_keyword))
          searchKeyWords.append(item.search_keyword+",")

        if(item.click_category_id!= -1 && !clickCategoryId.toString.contains(item.click_category_id))
          clickCategoryId.append(item.click_category_id+",")

        stepLength += 1
      }
      val visitLength = (DateUtils.parseTime(endTime).getTime - DateUtils.parseTime(startTime).getTime)/1000
      val searchKeyWordsStr = StringUtils.trimComma(searchKeyWords.toString)
      val clickCategoryIdStr = StringUtils.trimComma(clickCategoryId.toString)
      val aggrInfo = Constants.FIELD_SESSION_ID + "=" + session_id + "|" +
        Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKeyWordsStr + "|" +
        Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryIdStr + "|" +
        Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|" +
        Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|" +
        Constants.FIELD_START_TIME + "=" + startTime
      (userId,aggrInfo)
    }
    //group2AggrRdd.foreach(println(_))
    val sql = s"select * from user_info"
    import sparkSession.implicits._
    //userInfoRdd的类型为：RDD[UserInfo]，如果sparkSession.sql(sql).rdd，结果为RDD[Row]
    val userInfoRdd = sparkSession.sql(sql).as[UserInfo].rdd.map(item=>(item.user_id,item))
    //sessionAndUserRdd的类型为：RDD[(Long,(String,UserInfo))]
    val sessionAndUserRdd = group2AggrRdd.join(userInfoRdd)
    val fullInfoRdd = sessionAndUserRdd.map{ case(userId,(aggrInfo,userInfo))=>
      //注意函数第二个参数是"|" 还是"\\|"
      val sessionId = StringUtils.getFieldFromConcatString(aggrInfo,"\\|",Constants.FIELD_SESSION_ID)
      val fullInfo = aggrInfo + "|" + Constants.FIELD_AGE + "=" + userInfo.age + "|" +
        Constants.FIELD_PROFESSIONAL + "=" + userInfo.professional + "|" +
        Constants.FIELD_SEX + "=" + userInfo.sex + "|" +
        Constants.FIELD_CITY + "=" + userInfo.city

      (sessionId,fullInfo)
    }
    fullInfoRdd
  }

  def getResultByFilter(fullInfoRdd: RDD[(String, String)],taskJsonObj: JSONObject,acc:SessionAccumulator)={
    val startAge = ParamUtils.getParam(taskJsonObj,Constants.PARAM_START_AGE)
    val endAge = ParamUtils.getParam(taskJsonObj,Constants.PARAM_END_AGE)
    val professionals = ParamUtils.getParam(taskJsonObj,Constants.PARAM_PROFESSIONALS)
    val cities = ParamUtils.getParam(taskJsonObj,Constants.PARAM_CITIES)
    val sex = ParamUtils.getParam(taskJsonObj,Constants.PARAM_SEX)
    val keyWords = ParamUtils.getParam(taskJsonObj,Constants.PARAM_KEYWORDS)
    val category = ParamUtils.getParam(taskJsonObj,Constants.PARAM_CATEGORY_IDS)

    var filterInfo = (if(startAge!=null) Constants.PARAM_START_AGE + "=" + startAge + "|" else "") +
      (if(endAge!=null) Constants.PARAM_END_AGE + "=" + endAge + "|" else "") +
      (if(professionals!=null) Constants.PARAM_PROFESSIONALS + "=" + professionals + "|" else "") +
      (if(cities!=null) Constants.PARAM_CITIES + "=" + cities + "|" else "") +
      (if(sex!=null) Constants.PARAM_SEX + "=" + sex + "|" else "") +
      (if(keyWords!=null)Constants.PARAM_KEYWORDS + "=" + keyWords + "|" else "" ) +
      (if(category!=null)Constants.PARAM_CATEGORY_IDS + "=" + category + "|" else "" )
    if(filterInfo.endsWith("\\|"))
      filterInfo = filterInfo.substring(0,filterInfo.length-1)
    val sessionUserRdd = fullInfoRdd.filter{case (sessionId,item)=>
      var success = true
      if(!ValidUtils.between(item,Constants.FIELD_AGE,filterInfo,Constants.PARAM_START_AGE,Constants.PARAM_END_AGE))
        success = false
      else if(!ValidUtils.in(item,Constants.FIELD_PROFESSIONAL,filterInfo,Constants.PARAM_PROFESSIONALS))
        success = false
      else if(!ValidUtils.in(item,Constants.FIELD_CITY,filterInfo,Constants.PARAM_CITIES))
        success = false
      else if(!ValidUtils.equal(item,Constants.FIELD_SEX,filterInfo,Constants.PARAM_SEX))
        success = false
      else if(!ValidUtils.in(item,Constants.FIELD_SEARCH_KEYWORDS,filterInfo,Constants.PARAM_KEYWORDS))
        success = false
      else if(!ValidUtils.in(item,Constants.FIELD_CATEGORY_ID,filterInfo,Constants.PARAM_CATEGORY_IDS))
        success = false


      if(success==true){
        //统计总共的session数
        acc.add(Constants.SESSION_COUNT)
        //统计所有session
        val visitLength = StringUtils.getFieldFromConcatString(item,"\\|",Constants.FIELD_VISIT_LENGTH).toInt
        visitLengthCount(visitLength,acc)

        val stepLength = StringUtils.getFieldFromConcatString(item,"\\|",Constants.FIELD_STEP_LENGTH).toInt
        stepLengthCount(stepLength,acc)
      }
      success
    }
    sessionUserRdd
  }



  def visitLengthCount(visitLength:Int,acc: SessionAccumulator)={
    if(visitLength>=1 && visitLength<=3)
      acc.add(Constants.TIME_PERIOD_1s_3s)
    else if(visitLength>=4 && visitLength<=6)
      acc.add(Constants.TIME_PERIOD_4s_6s)
    else if(visitLength>=7 && visitLength<=9)
      acc.add(Constants.TIME_PERIOD_7s_9s)
    else if(visitLength>=10 && visitLength<=30)
      acc.add(Constants.TIME_PERIOD_10s_30s)
    else if(visitLength>30 && visitLength<=60)
      acc.add(Constants.TIME_PERIOD_30s_60s)
    else if(visitLength>60 && visitLength<=180)
      acc.add(Constants.TIME_PERIOD_1m_3m)
    else if(visitLength>180 && visitLength<=600)
      acc.add(Constants.TIME_PERIOD_3m_10m)
    else if(visitLength>600 && visitLength<=1800)
      acc.add(Constants.TIME_PERIOD_10m_30m)
    else if(visitLength>1800)
      acc.add(Constants.TIME_PERIOD_30m)
  }
  def stepLengthCount(stepLength: Int, acc: SessionAccumulator)={
    if(stepLength>=1&&stepLength<=3)
      acc.add(Constants.STEP_PERIOD_1_3)
    else if(stepLength>=4&&stepLength<=6)
      acc.add(Constants.STEP_PERIOD_4_6)
    else if(stepLength>=7&&stepLength<=9)
      acc.add(Constants.STEP_PERIOD_7_9)
    else if(stepLength>=10&&stepLength<=30)
      acc.add(Constants.STEP_PERIOD_10_30)
    else if(stepLength>30&&stepLength<=60)
      acc.add(Constants.STEP_PERIOD_30_60)
    else if(stepLength>60)
      acc.add(Constants.STEP_PERIOD_60)
  }

  def writeRate2DB(spark: SparkSession, value: mutable.HashMap[String, Int],taskUUID:String) = {
    val session_count = value.getOrElse(Constants.SESSION_COUNT,1).toDouble

    val visit_length_1s_3s = value.getOrElse(Constants.TIME_PERIOD_1s_3s, 0)
    val visit_length_4s_6s = value.getOrElse(Constants.TIME_PERIOD_4s_6s, 0)
    val visit_length_7s_9s = value.getOrElse(Constants.TIME_PERIOD_7s_9s, 0)
    val visit_length_10s_30s = value.getOrElse(Constants.TIME_PERIOD_10s_30s, 0)
    val visit_length_30s_60s = value.getOrElse(Constants.TIME_PERIOD_30s_60s, 0)
    val visit_length_1m_3m = value.getOrElse(Constants.TIME_PERIOD_1m_3m, 0)
    val visit_length_3m_10m = value.getOrElse(Constants.TIME_PERIOD_3m_10m, 0)
    val visit_length_10m_30m = value.getOrElse(Constants.TIME_PERIOD_10m_30m, 0)
    val visit_length_30m = value.getOrElse(Constants.TIME_PERIOD_30m, 0)

    val step_length_1_3 = value.getOrElse(Constants.STEP_PERIOD_1_3, 0)
    val step_length_4_6 = value.getOrElse(Constants.STEP_PERIOD_4_6, 0)
    val step_length_7_9 = value.getOrElse(Constants.STEP_PERIOD_7_9, 0)
    val step_length_10_30 = value.getOrElse(Constants.STEP_PERIOD_10_30, 0)
    val step_length_30_60 = value.getOrElse(Constants.STEP_PERIOD_30_60, 0)
    val step_length_60 = value.getOrElse(Constants.STEP_PERIOD_60, 0)

    val visit_length_1s_3s_ratio = NumberUtils.formatDouble(visit_length_1s_3s / session_count, 2)
    val visit_length_4s_6s_ratio = NumberUtils.formatDouble(visit_length_4s_6s / session_count, 2)
    val visit_length_7s_9s_ratio = NumberUtils.formatDouble(visit_length_7s_9s / session_count, 2)
    val visit_length_10s_30s_ratio = NumberUtils.formatDouble(visit_length_10s_30s / session_count, 2)
    val visit_length_30s_60s_ratio = NumberUtils.formatDouble(visit_length_30s_60s / session_count, 2)
    val visit_length_1m_3m_ratio = NumberUtils.formatDouble(visit_length_1m_3m / session_count, 2)
    val visit_length_3m_10m_ratio = NumberUtils.formatDouble(visit_length_3m_10m / session_count, 2)
    val visit_length_10m_30m_ratio = NumberUtils.formatDouble(visit_length_10m_30m / session_count, 2)
    val visit_length_30m_ratio = NumberUtils.formatDouble(visit_length_30m / session_count, 2)

    val step_length_1_3_ratio = NumberUtils.formatDouble(step_length_1_3 / session_count, 2)
    val step_length_4_6_ratio = NumberUtils.formatDouble(step_length_4_6 / session_count, 2)
    val step_length_7_9_ratio = NumberUtils.formatDouble(step_length_7_9 / session_count, 2)
    val step_length_10_30_ratio = NumberUtils.formatDouble(step_length_10_30 / session_count, 2)
    val step_length_30_60_ratio = NumberUtils.formatDouble(step_length_30_60 / session_count, 2)
    val step_length_60_ratio = NumberUtils.formatDouble(step_length_60 / session_count, 2)


    val stat = SessionAggrStat(taskUUID, session_count.toInt, visit_length_1s_3s_ratio, visit_length_4s_6s_ratio, visit_length_7s_9s_ratio,
      visit_length_10s_30s_ratio, visit_length_30s_60s_ratio, visit_length_1m_3m_ratio,
      visit_length_3m_10m_ratio, visit_length_10m_30m_ratio, visit_length_30m_ratio,
      step_length_1_3_ratio, step_length_4_6_ratio, step_length_7_9_ratio,
      step_length_10_30_ratio, step_length_30_60_ratio, step_length_60_ratio)

    val sessionRatioRdd = spark.sparkContext.makeRDD(Array(stat))
    import spark.implicits._
    sessionRatioRdd.toDF.
      write.
      format("jdbc").
      option("url",ConfigurationManager.config.getString(Constants.JDBC_URL)).
      option("user",ConfigurationManager.config.getString(Constants.JDBC_USER)).
      option("password",ConfigurationManager.config.getString(Constants.JDBC_PASSWORD)).
      option("dbtable","session_stat_ratio_0416").
      mode(SaveMode.Append).
      save()
  }
}
