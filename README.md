#### 项目构架

基于Kafka+Spark Streaming 进行是实时计算

日志数据从 日志服务器传入Kafka 后对接Spark Streaming 进行实时计算后存入到HBase，进行可视化展示

业务数据 由Canal 监控MySQL数据变化，传入Kafka 

(.\image\构架图.png)

项目构架图

#### 需求

1. 日活分时趋势图，及昨日对比
2. GMV趋势图，及昨日对比
3. 风险预警
4. 用户购买明细灵活分析

#### 关键点

##### 	MySQL Binlog 类型

​		使用row 此构架中只能选用次方式 Canal是伪Slave 不具备解析sql的能力。

##### 	Kafka-SparkStreaming对接

​		采用DirAPI ：每个Executor 对接一个borker 及负责接收也负责计算。

