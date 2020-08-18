package com.dake.app

import java.util

import com.dake.bean.CouponAlertInfo
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.util.control.Breaks.{break, breakable}

object TestApp {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("AlterApp").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    val eventLogDStream: RDD[(Int, (String, String))] =
      sc.makeRDD(Array((1, ("coupon", "u1")),
        (1, ("coupon", "u2")),
        (1, ("coupon", "u3")),
        (1, ("coupon", "u4")),
        (1, ("coupon", "u3")),
        (2, ("coupon", "u4")),
        (2, ("coupon", "u5")),
        (2, ("coupon", "u6")),
        (3, ("coupon", "u6")),
        (3, ("coupon", "u6")),
        (2, ("click", "u7"))))


    val value = eventLogDStream
      .groupByKey()
      .mapValues(iter => {
        val uidSet = new util.HashSet[String]()
        var flag: Boolean = true
        for (elem <- iter) {
          if (elem._1.equals("coupon")) {
            uidSet.add(elem._2)
          } else if (elem._1.equals("click")) {
            flag = false
          }

        }
        if (uidSet.size() >= 3 && flag) {
          null
        } else {
          iter
        }
      }).filter( _._2 == null)
    value.foreach(print)

  }
}
