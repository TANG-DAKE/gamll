package com.dake.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.dake.bean.EventLog
import com.dake.utils.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.codehaus.janino.Java.BreakableStatement
import scala.util.control.Breaks._


object AlterApp {
  def main(args: Array[String]): Unit = {
     val sparkConf: SparkConf = new SparkConf().setAppName("AlterApp").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] =
      MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_EVENT, ssc)
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH")
    val eventLogDStream: DStream[EventLog] = kafkaDStream.map(record => {
      val eventLog: EventLog = JSON.parseObject(record.value(), classOf[EventLog])
      val dateLogArr: Array[String] = sdf.format(new Date(eventLog.ts)).split(" ")
      eventLog.logDate = dateLogArr(0)
      eventLog.logHour = dateLogArr(1)
      eventLog
    })


    ssc.start()
    ssc.awaitTermination()
  }
}