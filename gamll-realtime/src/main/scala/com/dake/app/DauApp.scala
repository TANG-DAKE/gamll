package com.dake.app

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.dake.bean.StartUpLog
import com.dake.handler.DauHandler
import com.dake.utils.MyKafkaUtil
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._


object DauApp {
  def main(args: Array[String]): Unit = {
    //1创建SparkConf

    val sparkConf: SparkConf = new SparkConf().setAppName("dauApp").setMaster("local[*]")

    //2。创建StreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //3.读取kafka Start主题
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_STARTUP, ssc)

    //4.将读取的数据转换为样例类对象（log DATe和LogHour）
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH")
    val startLogDStream = kafkaDStream.map(record => {
      val vlaue: String = record.value()

      val startUpLog = JSON.parseObject(vlaue, classOf[StartUpLog])
      val ts = startUpLog.ts
      val dateHour = sdf.format(new Date(ts))
      val dateHourArr = dateHour.split(" ")
      startUpLog.logDate = dateHourArr(0)
      startUpLog.logHour = dateHourArr(1)
      startUpLog
    })
    //startLogDStream.cache()
    //startLogDStream.cache().print()

    //5.去重 根据redis中保存的数据进行跨次去重
    val filterByRedisDStream: DStream[StartUpLog] = DauHandler.filterByRedis(startLogDStream, ssc.sparkContext)
    //filterByRedisDStream.cache()
    //filterByRedisDStream.cache().print()

    //6.去重后的数据做同批次去中
    val filterByGroup: DStream[StartUpLog] = DauHandler.filterByGroup(filterByRedisDStream)
    filterByGroup.cache()

    //7.将两次去重后得数据写入redis（mid）
    DauHandler.saveMidToRedis(filterByGroup)

    //8.数据保存在HBase（Phoenix）
    filterByGroup.foreachRDD(rdd => {
      rdd.saveToPhoenix("GMALL0317_DAU",
        Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS"),
        HBaseConfiguration.create(),
        Some("hadoop102,hadoop103,hadoop104:2181"))
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
