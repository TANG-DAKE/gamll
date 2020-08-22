package com.dake.app

import java.time.LocalDate
import java.util

import com.alibaba.fastjson.JSON
import com.dake.bean.{OrderDetail, OrderInfo, SaleDetail, UserInfo}
import com.dake.utils.{MyESUtil, MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis
//scala 样例类转换json
import org.json4s.native.Serialization
//java集合 scala集合转换
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

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

    val orderIdToInfoAndDetailDStream: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] =
      orderInfoDStream.fullOuterJoin(orderDetailDStream)

    //使用mapPartition
    val noUserSaleDetail: DStream[SaleDetail] = orderIdToInfoAndDetailDStream.mapPartitions(iter => {

      val jedisClient: Jedis = RedisUtil.getJedisClient
      //创建集合用于存放JOIN上的数据 （当前批，及前置批）
      val details = new ListBuffer[SaleDetail]
      implicit val formats = org.json4s.DefaultFormats

      iter.foreach { case ((orderId, (infoOpt, detailOpt))) =>
        //定义info&detail胡存入Redis中的key
        val infoRedisKey = s"order_info:$orderId"
        val detailRedisKey = s"order_detail:$orderId"
        if (infoOpt.isDefined) {
          //info有值
          val orderInfo: OrderInfo = infoOpt.get
          //1.1detail也有值
          if (detailOpt.isDefined) {
            val orderDetail: OrderDetail = detailOpt.get
            details += new SaleDetail(orderInfo, orderDetail)
          }
          //1.2 将order Info转换为JSON 字符串写入redis
          //JSON.toJSONString(orderInfo) 编译报错
          val orderInfoJson: String = Serialization.write(orderInfo)
          jedisClient.set(infoRedisKey, orderInfoJson) //String 类型
          jedisClient.expire(infoRedisKey, 100)

          //1.3 orderId 查询orderDetail前置批次数据
          val orderDetailJsonSet: util.Set[String] = jedisClient.smembers(detailRedisKey)

          orderDetailJsonSet.asScala.foreach(orderDetailJson => {
            val orderDetail: OrderDetail = JSON.parseObject(orderDetailJson, classOf[OrderDetail])
            details += new SaleDetail(orderInfo, orderDetail)
          })
        } else {
          //info无值
          val orderDetail: OrderDetail = detailOpt.get
          //查蓄奴orderInfo前置批次数据
          val orderInfoJson: String = jedisClient.get(infoRedisKey)
          if (orderInfoJson != null) {
            //转换为样例类对象
            val orderInfo: OrderInfo = JSON.parseObject(orderInfoJson, classOf[OrderInfo])
            details += new SaleDetail(orderInfo, orderDetail)
          } else {
            val orderDetailJson: String = Serialization.write(orderDetail)
            jedisClient.sadd(detailRedisKey, orderDetailJson)
            jedisClient.expire(detailRedisKey, 100)
          }
        }
      }
      jedisClient.close()
      details.toIterator
    })


    val saleDetailDStream: DStream[SaleDetail] = noUserSaleDetail.mapPartitions(iter => {
      val jedisClient: Jedis = RedisUtil.getJedisClient
      val details: Iterator[SaleDetail] = iter.map { noUserSaleDetail => {
        val userInfoJson: String = jedisClient.get(s"user_Info:${noUserSaleDetail.user_id}")
        //将json转换为样例类
        val userInfo: UserInfo = JSON.parseObject(userInfoJson, classOf[UserInfo])
        noUserSaleDetail.mergeUserInfo(userInfo)

        noUserSaleDetail
      }
      }

      jedisClient.close()
      details
    })

    saleDetailDStream.print(100)
    saleDetailDStream.foreachRDD(rdd => {
      rdd.foreachPartition(iter => {
        val today: String = LocalDate.now().toString
        val indexName = s"${GmallConstants.ES_SALE_DETAIL}-$today"

        val detailIdSaleDetailIter: Iterator[(String, SaleDetail)] = iter.map(saleDetail => (saleDetail.order_detail_id, saleDetail))

        MyESUtil.insertByBulk(indexName,"_doc",detailIdSaleDetailIter.toList)
      })
    })

    ssc.start()
    ssc.awaitTermination()

  }

}
