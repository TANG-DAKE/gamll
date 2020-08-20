package com.dake.utils

import java.util.Objects

import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.core.{Bulk, Index}

object MyESUtil {
  private val ES_HOST = "http://hadoop102"
  private val ES_HTTP_PORT = 9200
  private var factory: JestClientFactory = null

  /**
   * 获取客户端
   *
   * @return jestclient
   */
  def getClient: JestClient = {
    if (factory == null) build()
    factory.getObject
  }

  /**
   * 关闭客户端
   */
  def close(client: JestClient): Unit = {
    if (!Objects.isNull(client)) try
      client.shutdownClient()
    catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }

  /**
   * 建立连接
   */
  private def build(): Unit = {
    factory = new JestClientFactory
    factory.setHttpClientConfig(
      new HttpClientConfig
      .Builder(ES_HOST + ":" + ES_HTTP_PORT)
        .multiThreaded(true)
        .maxTotalConnection(20) //连接总数
        .connTimeout(10000)
        .readTimeout(10000)
        .build)
  }

  //批量插入数据到ES
  def insertByBulk(indexName: String, typeName: String, list: List[(String, Any)]): Unit = {
    if (list.nonEmpty) {
      val client: JestClient = getClient
      //创建Bulk.Builder 对象
      val builder: Bulk.Builder = new Bulk.Builder()
        .defaultIndex(indexName)
        .defaultType(typeName)

      list.foreach { case (docId, data) => {
        val index: Index = new Index.Builder(data)
          .id(docId)
          .build()

        builder.addAction(index)
      }
      }
      val bulk: Bulk = builder.build()
      //提交 关闭
      client.execute(bulk)
      close(client)
    }

  }

}
