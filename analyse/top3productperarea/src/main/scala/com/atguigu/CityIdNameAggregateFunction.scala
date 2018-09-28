package com.atguigu

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType}

class CityIdNameAggregateFunction extends UserDefinedAggregateFunction{
  override def inputSchema: StructType = StructType(StructField("cityIdNameStr",StringType)::Nil)

  override def bufferSchema: StructType = StructType(StructField("cityIdNameStr",StringType)::Nil)

  override def dataType: DataType = StringType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = ""
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    var baseInfo = buffer.getString(0)
    val inputInfo = input.getString(0)
    if(!baseInfo.contains(inputInfo)){
      if("".equals(baseInfo)){
        baseInfo += inputInfo
      }else{
        baseInfo += ","+inputInfo
      }
      buffer.update(0,baseInfo)
    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    var buffer1Info = buffer1.getString(0)
    val buffer2Info = buffer2.getString(0)

    for(dataStr<-buffer2Info.split(",")){
      if(!buffer1Info.contains(dataStr)){
        if("".equals(buffer1Info)){
          buffer1Info += dataStr
        }else{
          buffer1Info += ","+dataStr
        }
      }
    }
    buffer1.update(0,buffer1Info)
  }

  override def evaluate(buffer: Row): Any = {
    buffer.getString(0)
  }
}
