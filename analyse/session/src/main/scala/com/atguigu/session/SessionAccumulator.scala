package com.atguigu.session

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

class SessionAccumulator extends AccumulatorV2[String,mutable.HashMap[String,Int]]{
  val sessionCountMap = new mutable.HashMap[String,Int]()

  override def isZero: Boolean = {
    this.sessionCountMap.isEmpty
  }

  override def copy(): AccumulatorV2[String, mutable.HashMap[String,Int]] = {
    val acc = new SessionAccumulator()
    //这个地方用 +=行不行
    acc.sessionCountMap ++= this.sessionCountMap
    acc
  }

  override def reset(): Unit = {
    this.sessionCountMap.clear()
  }

  override def add(v: String): Unit = {
    if(!this.sessionCountMap.contains(v))
      this.sessionCountMap.put(v,0)
    this.sessionCountMap.put(v,this.sessionCountMap(v)+1)
  }

  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Int]]): Unit = {
    other match{
      case acc:SessionAccumulator =>acc.sessionCountMap.foldLeft(this.sessionCountMap){
        case (map,(k,v))=>map += (k->(map.getOrElse(k,0)+v))
      }
    }
  }

  override def value: mutable.HashMap[String, Int] = {
    this.sessionCountMap
  }
}
