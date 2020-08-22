package com.dake.app;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.dake.utils.MyKafkaSender;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Random;

public class CanalClient {
    public static void main(String[] args) {
        //获取连接器
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(
                new InetSocketAddress("hadoop102", 11111),
                "example",
                "",
                "");

        while (true) {
            //连接
            canalConnector.connect();
            //订阅监控的表
            canalConnector.subscribe("gmall200317.*");

            //抓取数据
            Message message = canalConnector.get(100);
            //判断当前是否抓取到数据
            if (message.getEntries().size() <= 0) {
                System.out.println("当前抓取没有数据，休息5s");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }


            //解析message
            //获取message中的entry集合并遍历
            for (CanalEntry.Entry entry : message.getEntries()) {
                //获取entry、中ROWDATA类型数据
                if (CanalEntry.EntryType.ROWDATA.equals(entry.getEntryType())) {
                    //提取  表名  数据
                    try {
                        String tableName = entry.getHeader().getTableName();
                        ByteString storeValue = entry.getStoreValue();
                        //反序列化storeValue
                        CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(storeValue);
                        //获取事件类型
                        CanalEntry.EventType eventType = rowChange.getEventType();
                        //获取数据
                        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
                        //处理数据
                        handler(tableName, eventType, rowDatasList);
                    } catch (InvalidProtocolBufferException e) {
                        e.printStackTrace();
                    }

                }

            }

        }
    }


    //根据表名 事件类型及数据将数据发送至指定主题
    private static void handler(String tableName, CanalEntry.EventType eventType, List<CanalEntry.RowData> rowDatasList) {
        //GMV需求只需要order_info表中的新增数据
        if ("order_info".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType)) {

            sendToKadka(rowDatasList, GmallConstants.KAFKA_TOPIC_ORDER_INFO);

            //order_detail 新增
        } else if ("order_detail".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType)) {

            sendToKadka(rowDatasList, GmallConstants.KAFKA_TOPIC_ORDER_DETAIL);

            //用户信息 新增及变化
        } else if ("user_info".equals(tableName) && (CanalEntry.EventType.INSERT.equals(eventType) ||
                CanalEntry.EventType.UPDATE.equals(eventType))) {

            sendToKadka(rowDatasList, GmallConstants.KAFKA_TOPIC_USER_INFO);
        }
    }

    private static void sendToKadka(List<CanalEntry.RowData> rowDatasList, String topic) {


        for (CanalEntry.RowData rowData : rowDatasList) {
            //创建JSON对象用于存放列级对象
            JSONObject jsonObject = new JSONObject();

            for (CanalEntry.Column column : rowData.getAfterColumnsList()) {
                jsonObject.put(column.getName(), column.getValue());
            }
            //增加随机延迟
           /* try {
                Thread.sleep(new Random().nextInt(3*1000));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }*/

            System.out.println(jsonObject.toString());
            MyKafkaSender.send(topic, jsonObject.toString());
        }
    }
}
