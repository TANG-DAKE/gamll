package com.dake.app

import java.text.SimpleDateFormat
import java.time.LocalDate
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.dake.bean.{CouponAlertInfo, EventLog}
import com.dake.utils.{MyESUtil, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import scala.util.control.Breaks._


object AlterApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("AlterApp").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] =
      MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_EVENT, ssc)

    val sdf = new SimpleDateFormat("yyyy-MM-dd HH")

    val eventLogDStream: DStream[EventLog] = kafkaDStream.map(record => {
      //转换样例类
      val eventLog: EventLog = JSON.parseObject(record.value(), classOf[EventLog])

      val dateLogArr: Array[String] = sdf.format(new Date(eventLog.ts)).split(" ")
      eventLog.logDate = dateLogArr(0)
      eventLog.logHour = dateLogArr(1)
      eventLog
    })

    //开窗
    val windowDStream: DStream[EventLog] = eventLogDStream.window(Minutes(5))

    val couponAlterInfoDStream: DStream[CouponAlertInfo] = windowDStream
      .map(eventLog => (eventLog.mid, eventLog))
      .groupByKey()
      .map {
        case (mid, logIter) => {
          //用于存放领券的Uid
          val uidSet = new util.HashSet[String]()
          val itemIds = new util.HashSet[String]()
          var noClick = true
          val events = new util.ArrayList[String]()

          breakable(
            for (eventLog <- logIter) {

              val evid: String = eventLog.evid
              events.add(evid)

              if (evid.equals("coupon")) {
                uidSet.add(eventLog.uid)
                itemIds.add(eventLog.itemid)
              } else if (evid.equals("clickItem")) {
                noClick = false
                break()
              }
            }
          )

          //生成预警日志
          if (uidSet.size() > 3 && noClick) {
            CouponAlertInfo(mid, uidSet, itemIds, events, System.currentTimeMillis())
          } else {
            null
          }
        }
      }.filter(x => x != null)

    couponAlterInfoDStream.cache()
    couponAlterInfoDStream.print()

    //生成的日志写入ES
    couponAlterInfoDStream
      .foreachRDD(rdd => {

        rdd.foreachPartition(iter => {

          val docIdDate: Iterator[(String, CouponAlertInfo)] = iter.map(alterInfo => {
            val minutes: Long = alterInfo.ts / 1000 / 60
            (s"${alterInfo.mid}-$minutes", alterInfo)
          })

          val date: String = LocalDate.now().toString
          MyESUtil.insertByBulk(
            GmallConstants.ES_INDEX_ALTER + "_" + date,
            "_doc",
            docIdDate.toList)
        })
      })

    ssc.start()
    ssc.awaitTermination()
  }
}