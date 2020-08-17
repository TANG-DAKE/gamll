package com.dake.handler

import java.{lang, util}
import java.time.LocalDate

import com.dake.bean.StartUpLog
import com.dake.utils.RedisUtil
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.DStream

object DauHandler {

  //批次内去重
  def filterByGroup(filterByRedisDStream: DStream[StartUpLog]): DStream[StartUpLog] = {
    //转换结构
    val value = filterByRedisDStream.map(startLog => {
      (s"${startLog.mid}-${startLog.logDate}", startLog)
    })
      .groupByKey()
      .mapValues(iter => {
        iter.toList.sortWith(_.ts < _.ts).take(1)
      })
      .flatMap {
        case (_, list) => list
      }
    value
  }

  //根据Redis进行去重
  def filterByRedis(startLogDStream: DStream[StartUpLog], sc: SparkContext): DStream[StartUpLog] = {
    //方案一：单条数据过滤
    val value1 = startLogDStream.filter(filterFunc = startLog => {
      //获取连接
      val jedisClient = RedisUtil.getJedisClient
      //查询是否存在此条数据
      val exist: lang.Boolean = jedisClient.sismember(s"dau:${startLog.logDate}", startLog.mid)
      //归还连接
      jedisClient.close()
      //返回值
      !exist
    })

    //方案二：使用分区操作代替单挑操作 减少连接数
    val value2 = startLogDStream.mapPartitions(iter => {
      //获取连接
      val jedisClient = RedisUtil.getJedisClient
      //过滤
      val filterIter = iter.filter(startLog => {
        //查询是否存在此条数据
        !jedisClient.sismember(s"dau:${startLog.logDate}", startLog.mid)
      })
      //归还连接
      jedisClient.close()
      //返回值
      filterIter
    })

    //方案三 使用每个批次获取一次Redis中的set集合数据，广播至Executor
    val value3 = startLogDStream.transform(rdd => {
      //获取redis并广播
      val jedisClient = RedisUtil.getJedisClient
      val today = LocalDate.now().toString
      val midSet = jedisClient.smembers(s"dau:$today")
      val midSetBC: Broadcast[util.Set[String]] = sc.broadcast(midSet)

      jedisClient.close()
      //在executor使用BC进行去重
      rdd.filter(startLog => {
        !midSetBC.value.contains(startLog.mid)
      })

    })

    //方法返回值
    value3
  }

  def saveMidToRedis(startLogDStream: DStream[StartUpLog]): Unit = {
    startLogDStream.foreachRDD(rdd => {
      //使用foreachPartition 代替foreach，减少链接的获取和释放
      rdd.foreachPartition(iter => {
        //获取连接
        val jedisClient = RedisUtil.getJedisClient
        //操作数据
        iter.foreach(startLog => {
          val redisKey = s"dau:${startLog.logDate}"
          jedisClient.sadd(redisKey, startLog.mid)
        })
        //释放连接
        jedisClient.close()

      })
    })
  }


}
