package com.atguigu

import java.util.Date

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.utils.DateUtils
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Duration, Minutes, Seconds, StreamingContext}
import org.codehaus.jackson.map.ext.CoreXMLDeserializers.DurationDeserializer

import scala.collection.mutable.ArrayBuffer

object AdverStat {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("adverstat").setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()

    // StreamingContext.getActiveOrCreate(checkpointPath, func),利用这种方式创建
    //的streamingContext,如果挂掉了，会去checkPoint的路径下利用恢复文件来恢复
    //streamingContext,如果没有找到恢复文件就利用func函数重新生成一个streamingContext
    val streamingContext = new StreamingContext(sparkSession.sparkContext,Seconds(5))

    val kafkaBrokers = ConfigurationManager.config.getString(Constants.KAFKA_BROKERS)
    val kafkaTopics = ConfigurationManager.config.getString(Constants.KAFKA_TOPICS)

    // 指定消费kafka数据时所有的配置参数
    val kafkaConsumerParam = Map(
      "bootstrap.servers" -> kafkaBrokers,
      "key.deserializer"  -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "group1",
      // "auto.offset.reset": latest  earlist  none
      // "latest" : 开始消费数据时，从Zookeeper获取offset信息，如果没有offset信息，消费最新的数据
      // "earlist" : 开始消费数据时，从Zookeeper获取offset信息，如果没有offset信息，从最早的数据开始消费
      // "none" : 开始消费数据时，从Zookeeper获取offset信息，如果没有offset信息，直接报错
      "auto.offset.reset" -> "latest",
      //是否自动提交offset
      "enable.auto.commit" -> (false:java.lang.Boolean)

    )
    //ConsumerStrategies消费策略，用来指定消费哪个topic,以及消费者参数的配置
    //LocationStrategies，这个策略用来决定executor去哪个kafka分区取数据，LocationStrategies.PreferConsistent是让kafka分区平均
    //分配给executor
    val adRealTimeDStream = KafkaUtils.createDirectStream[String,String](streamingContext,LocationStrategies.PreferConsistent
    ,ConsumerStrategies.Subscribe[String,String](Array(kafkaTopics),kafkaConsumerParam))

    val adRealTimeValueDStream = adRealTimeDStream.map{
      case message=>message.value()
    }

    //commit

    //adRealTimeFilterDStream：DStream[RDD[message]] message中的信息timestamp province city userid adid
    val adRealTimeFilterDStream = adRealTimeValueDStream.transform{
      rdd =>
        val blackUer = AdBlacklistDAO.findAll().map{
          adblackList =>adblackList.userid
        }
        rdd.filter{
          adverMessage =>
            val userId = adverMessage.split(" ")(3).toLong
            !blackUer.contains(userId)
        }
    }
    //需求七广告点击黑名单实时统计
    generateBlackListRealTime(adRealTimeFilterDStream)

    //需求八各省各城市广告点击量实时统计
    streamingContext.checkpoint("./streamingContext")
    adRealTimeFilterDStream.checkpoint(Duration(10000))
    val key2CountDStram = calculateProvinceCityClickCount(adRealTimeFilterDStream)

    //需求九 每天每个省份Top3热门广告
    getTop3AddPerProvince(key2CountDStram,sparkSession)

    //需求十 最近一小时广告点击量实时统计
    calculatePerHourPerMinuteAdverClickCount(adRealTimeFilterDStream)
    streamingContext.start()
    streamingContext.awaitTermination()


  }
  def calculatePerHourPerMinuteAdverClickCount(adRealTimeFilterDStream: DStream[String]) = {
    val timeAddDstream = adRealTimeFilterDStream.map{
          //timestamp province city userid adid
      key =>
        val keySplit = key.split(" ")
        val time = DateUtils.formatTimeMinute(new Date(keySplit(0).toLong))
        val adid = keySplit(4).toLong
        val newKey = time+"_"+adid
        (newKey,1L)
    }

    val key2CountDStream = timeAddDstream.reduceByKeyAndWindow((a:Long,b:Long)=>a+b,Minutes(60),Minutes(1))
    key2CountDStream.foreachRDD{
      rdd =>
        rdd.foreachPartition{
          // items: Iterable[(key, count)]
          // key: timeMinute_adid
          // timeMinute：yyyyMMddHHmm
          items =>
            val clickCountArray = new ArrayBuffer[AdClickTrend]()

            for((key, count) <- items){
              // key: timeMinute_adid
              // timeMinute：yyyyMMddHHmm
              val keySplit = key.split("_")
              val timeMinte = keySplit(0)
              val date = timeMinte.substring(0, 8)
              val hour = timeMinte.substring(8, 10)
              val minute = timeMinte.substring(10)
              val adid = keySplit(1).toLong

              clickCountArray += AdClickTrend(date, hour, minute, adid, count)
            }

            AdClickTrendDAO.updateBatch(clickCountArray.toArray)
        }
    }
  }

  def generateBlackListRealTime(adRealTimeFilterDStream: DStream[String]) = {
    val dateUserAdnumDStream = adRealTimeFilterDStream.map{
      message=>
        val mesSplit = message.split(" ")
        val date = DateUtils.formatDate(new Date(mesSplit(0).toLong))
        val userId = mesSplit(3)
        val adid = mesSplit(4)
        val dateUserAd = date + "_" + userId+ "_" + adid
        (dateUserAd,1)
    }
    //dateUserAdCountDstream:Dstream[RDD[(date_userId_adid),count]]
    val dateUserAdCountDstream = dateUserAdnumDStream.reduceByKey(_+_)

    dateUserAdCountDstream.foreachRDD{
      rdd =>
        rdd.foreachPartition{
          items=>
            val arrayBuffer = new ArrayBuffer[AdUserClickCount]()
            for((item,count)<-items){
              val mess = item.split("_")
              val date = mess(0)
              val userId = mess(1).toLong
              val adId = mess(2).toLong
              val adUserClickCount = AdUserClickCount(date,userId,adId,count)
              arrayBuffer += adUserClickCount
            }
            AdUserClickCountDAO.updateBatch(arrayBuffer.toArray)
        }
    }
    //blackListDstream:Dstream[rdd[(key,count)]]
    val blackListDstream = dateUserAdCountDstream.filter{
      case (key,count)=>
        //date_userId_adid
        val keySplit = key.split("_")
        val keyDate = keySplit(0)
        val userId = keySplit(1).toLong
        val adid = keySplit(2).toLong
        val num = AdUserClickCountDAO.findClickCountByMultiKey(keyDate,userId,adid)
        if(num>100)
          true
        else
          false
    }

    val blackDistListDstream = blackListDstream.transform{
      rdd=>
        val userId = rdd.map{
          case(key,count)=>{
            val keySplit = key.split("_")
            val userId = keySplit(1)
            userId
          }
        }
        userId.distinct()
    }
    blackDistListDstream.foreachRDD{
      rdd =>
        rdd.foreachPartition{
          items =>
            val arrayBuffer = new ArrayBuffer[AdBlacklist]()
            for(item<-items){
              arrayBuffer += AdBlacklist(item.toLong)
            }
            AdBlacklistDAO.insertBatch(arrayBuffer.toArray)
        }
    }
  }

  def calculateProvinceCityClickCount(adRealTimeFilterDStream: DStream[String]) = {

    val cityAd2NumRDD = adRealTimeFilterDStream.map{
      //timestamp province city userid adid
      item=>
        val keySplit = item.split(" ")
        val keyDate = DateUtils.formatDate(new Date(keySplit(0).toLong))
        val province = keySplit(1)
        val city = keySplit(2)
        val adid = keySplit(4).toLong
        val newKey = keyDate+"_"+province+"_"+city+"_"+adid
        (newKey,1L)
    }
    val cityAdCountRDD = cityAd2NumRDD.updateStateByKey[Long]{
      (values:Seq[Long],state:Option[Long])=>
        var sum = 0L
        if(state.isDefined){
          sum = state.get
        }
        for(value<-values)
          sum += value
        Some(sum)
    }

    cityAdCountRDD.foreachRDD{
      rdd =>
        rdd.foreachPartition{
          items =>
            val adStatBuffer = new ArrayBuffer[AdStat]()
            for((key,count)<-items){
            //key:keyDate_province_city_adid
            val keySplit = key.split("_")
            val date = keySplit(0)
            val province = keySplit(1)
            val city = keySplit(2)
            val adid = keySplit(3).toLong
            adStatBuffer += AdStat(date,province,city,adid,count)
          }
            AdStatDAO.updateBatch(adStatBuffer.toArray)
        }
    }
    cityAdCountRDD
  }

  def getTop3AddPerProvince(key2CountDStram: DStream[(String, Long)],sparkSession:SparkSession) = {
    //keyDate_province_city_adid
    val provinceAddNumDstream = key2CountDStram.map{
      case (key,count)=>
        val keySplit = key.split("_")
        val date = keySplit(0)
        val province = keySplit(1)
        val adid = keySplit(3).toLong
        val newKey = date+"_"+province+"_"+adid
        (newKey,count)
    }
   val provinceAddCountDstream = provinceAddNumDstream.reduceByKey(_+_)
    val AdProvinceTop3Dstream = provinceAddCountDstream.transform{
      rdd=>
        val tableRdd = rdd.map{
          case (key,count)  =>
            //key:date+"_"+province+"_"+adid
            val keySplit = key.split("_")
            val date = keySplit(0)
            val province = keySplit(1)
            val adid = keySplit(2).toLong
            (date,province,adid,count)
        }

        import sparkSession.implicits._
        tableRdd.toDF("date","province", "adid", "clickCount").createOrReplaceTempView("tmp_basic_info_table")
        val sql = "select date,province,adid,clickCount from (select date,province,adid,clickCount,row_number() over(partition by date,province order by clickCount desc) rank" +
          " from tmp_basic_info_table) temp where rank <=3"

        sparkSession.sql(sql).as[AdProvinceTop3].rdd
    }

    AdProvinceTop3Dstream.foreachRDD{
      rdd=>
        rdd.foreachPartition{
          items=>
            val AdProvinceTop3Array = new ArrayBuffer[AdProvinceTop3]()
            for(item<-items){
              AdProvinceTop3Array += item
            }
            AdProvinceTop3DAO.updateBatch(AdProvinceTop3Array.toArray)
        }
    }
  }

}
