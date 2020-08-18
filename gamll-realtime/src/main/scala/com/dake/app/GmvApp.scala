package com.dake.app

import com.alibaba.fastjson.JSON
import com.dake.bean.OrderInfo
import com.dake.utils.MyKafkaUtil
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._


object GmvApp {
  def main(args: Array[String]): Unit = {
    //创建sparkCong steamingContext
    val sparkConf: SparkConf = new SparkConf().setAppName("GmvApp").setMaster("local[*]")
    //读取kafka order_info主题创建流
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    //将数据转换为样例类：给日期小时字段重新赋值，给联系人手机号脱敏
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] =
      MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER_INFO, ssc)

    val orderInfoDStream: DStream[OrderInfo] = kafkaDStream.map(record => {
      //转化样例类
      val info: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])

      //日期/小时重新赋值
      val create_time: String = info.create_time
      val timeArr: Array[String] = create_time.split(" ")
      info.create_date = timeArr(0)
      info.create_hour = timeArr(1)

      //脱敏
      val telTuple: (String, String) = info.consignee_tel.splitAt(4)
      info.consignee_tel = telTuple._1 + "*******"
      info
    })
    orderInfoDStream.cache()
    orderInfoDStream.print()

    //写入Phoenix
    orderInfoDStream.foreachRDD(rdd=>{
      rdd.saveToPhoenix("GMALL200317_ORDER_INFO",
        classOf[OrderInfo].getDeclaredFields.map(_.getName.toUpperCase()),
        HBaseConfiguration.create(),
        Some("hadoop102,hadoop103,hadoop104:2181"))
    })

    //启动
    ssc.start()
    ssc.awaitTermination()

  }

}
