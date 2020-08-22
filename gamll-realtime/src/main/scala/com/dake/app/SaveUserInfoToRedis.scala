package com.dake.app

import com.alibaba.fastjson.JSON
import com.dake.bean.UserInfo
import com.dake.utils.{MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.InputDStream
import redis.clients.jedis.Jedis

object SaveUserInfoToRedis {
  def main(args: Array[String]): Unit = {
    val ssc = new StreamingContext(
      new SparkConf().setAppName("SaveUserInfoToRedis").setMaster("local[*]"),
      Seconds(3))

    val userInfoKafkaDStream: InputDStream[ConsumerRecord[String, String]] =
      MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_USER_INFO, ssc)

    userInfoKafkaDStream.foreachRDD(rdd => {
      rdd.foreachPartition(iter => {
        val jedisClient: Jedis = RedisUtil.getJedisClient
        iter.foreach(record => {
          val userInfoJson: String = record.value()
          val userInfo: UserInfo = JSON.parseObject(userInfoJson, classOf[UserInfo])
          val userRedisKey = s"user_info:${userInfo.id}"
          jedisClient.set(userRedisKey, userInfoJson)
        })
        jedisClient.close()
      })
    })
    ssc.start()
    ssc.awaitTermination()
  }

}
