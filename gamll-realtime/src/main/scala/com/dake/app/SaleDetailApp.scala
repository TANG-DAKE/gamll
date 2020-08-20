package com.dake.app

import com.alibaba.fastjson.JSON
import com.dake.bean.{OrderDetail, OrderInfo}
import com.dake.utils.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SaleDetailApp {
  def main(args: Array[String]): Unit = {
    val ssc = new StreamingContext(
      new SparkConf().setAppName("SaleDetailApp").setMaster("local[*]"),
      Seconds(5))

    val orderInfoKafkaDStream: InputDStream[ConsumerRecord[String, String]] =
      MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER_INFO, ssc)
    val orderDetailKafkaDStream: InputDStream[ConsumerRecord[String, String]] =
      MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER_DETAIL, ssc)

    val orderInfoDStream: DStream[(String, OrderInfo)] = orderInfoKafkaDStream.map(record => {

      //转化OrderInfo对象
      val info: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])

      //日期/小时重新赋值
      val create_time: String = info.create_time
      val timeArr: Array[String] = create_time.split(" ")
      info.create_date = timeArr(0)
      info.create_hour = timeArr(1).split(":")(0)

      //脱敏
      val telTuple: (String, String) = info.consignee_tel.splitAt(4)
      info.consignee_tel = telTuple._1 + "*******"
      (info.id, info)
    })

    val orderDetailDStream: DStream[(String, OrderDetail)] = orderDetailKafkaDStream.map(record => {
      //转化样例类
      val infoDetail: OrderDetail = JSON.parseObject(record.value(), classOf[OrderDetail])

      (infoDetail.order_id, infoDetail)
    })
  }

}
