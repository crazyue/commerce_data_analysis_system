package com.atguigu.session

case class SortKey(clickCount:Long,orderCount:Long,payCount:Long) extends Ordered[SortKey]{
  override def compare(that: SortKey) = {
    var flag = 0
    if(this.clickCount - that.clickCount !=0)
      flag = (this.clickCount - that.clickCount).toInt
    else if(this.orderCount - that.orderCount != 0)
      flag = (this.orderCount - that.orderCount).toInt
    else if(this.payCount - that.payCount != 0)
      flag = (this.payCount - that.payCount).toInt
    flag
  }
}
