#### 项目构架

基于Kafka+Spark Streaming 进行是实时计算

日志数据从 日志服务器传入Kafka 后对接Spark Streaming 进行实时计算后存入到HBase，进行可视化展示

业务数据 由Canal 监控MySQL数据变化，传入Kafka 


![构架图](image\构架图.png)
=======
项目构架图

#### 需求

1. 日活分时趋势图，及昨日对比
2. GMV趋势图，及昨日对比
3. 风险预警
4. 用户购买明细灵活分析



#### 需求分析：

​                    ***日活需求***
数据流：用户行为数据 -> HBase (Phoenix)
去重：（1）跨批次去重  Redis 
​     				 1 我们要存什么 按天存 mid
​    				  2 数据类型选择  set
​     				 3 rediskey 设计      s：“dau：$logdate"：Set[mid]

​	  		(2) 同批次数据去重 groupByKey()  mapValue() sortWith() take()

为什么选用HBase（ES）： 明细（去重后的数据）
      （1）可以计算不同维度的需求
      （2）精准一次性，幂等性  （多次写如同一条数据覆盖）
      （3）数据量大

MySQL：结果数据（计算完成）    
      （1） 只能算固定的需求
      （2）精准一次性，事务（写入失败后回滚）



​                ***GMV***
数据流：用户业务数据--->HBase（ES）
采集业务数据（Canal）--采集mysql 中变化的数据



​             ***购物券功能风险预警***
数据流：用户行为数据 -->ES
（1）Spark Streaming对接ES
（2）Kibana 可视化

同一设备5分中内三次及以上用不同账号登录并领券，并且过程中没有浏览行为
达到以上要求则产生一条预警日志。并且同一设备，每分钟只记录一次

同一设备 ：mid  groupByKey
5分钟内 ： 开窗 windows()
三个账号：uid 去重  使用set集合
过滤有浏览的行为的设备: filter 
记录一次 存入ES     mid_2020-08-17_14:20    |    docid



#### 关键点

##### 	MySQL Binlog 类型

​		使用row 此构架中只能选用次方式 Canal是伪Slave 不具备解析sql的能力。

##### 	Kafka-SparkStreaming对接

​		采用DirectAPI ：每个Executor 对接一个borker 及负责接收也负责计算。



### 理论

#### Kafka:

##### 	为什么选用kafka作为消息队列： 

1. kafka 可以进行动态拓展当有新的需求时，增加消费者组即可，无需停机。
2. 快速持久化。
3. 高吞吐量：普通的服务器即可达到10W/s的吞吐量
4. 完全的分布式系统：Broker、Producer和Consumer都是原生自动支持分布式，自动实现负载均衡（分区）
5. 支持数据批量发送拉取
6. 支持同步，异步复制两种高可用机制。

#####    如何实现高速读写：

1. 0拷贝技术
2. 顺序写磁盘

#####     文件存储机制

​	topic 分为多个partition 每个partition 分为多个segment，每个segment又分为 .log(数据) 和.index(offset)

#####     生产者

1. 分区策略  
   - 未指定分区使用key的hash/分区数作为分区号 
   - 未指定partition和key 采用粘性分区，随机选择一个分区直到Batch满 或者完成 再随机一个分区
2. 数据可靠性保证
   - ack 0   1   -1  同步消息后应答的时机 （ISR 同步消息组） 0 leader收到消息  1 leader 落盘 -1 leaderhefollower都落盘
   - Exactly Once 当是设置ack= -1 和At Least once + 幂等性 可以保证 数据可靠

#####     消费者

1. 分区策略
   - range 范围分区
   - roundRobin 轮询分区
2. offset维护
   - 0.9前维护在Zookeeper 之后维护在_consumer_offset主题
3. 精准一次消费
   - 使用关系型数据库进行事务处理。将消费和offset进行原子绑定。消费者返回offset 保存至支持事务的自定义介质（MySQL）
   - 手动提交偏移量+幂等性处理（不靠谱）



#### SparkStreaming：

#####     DStream离散化流：

#####     构架：

- 由一个Execuotr（Receiver）接收数据，其余Executor执行计算
- 背压机制：根据计算的速度动态的调整接收速率

#####    注意点：

1. 禁止使用checkpoint 在重启服务时会将停机时的全部数据一次性提交，集群崩溃。
2. DirectAPI ：Executor读取并计算 增加Executor怎加并行度
3. ReceiverAPI： 专门的接受数据其余计算（速度不统一） 跨界点传输存在WAL 效率低

#####     sparkCore 基于内存的快速、通用、可拓展的大数据分析引擎:

1. Driver 
   - 创建SparkContext
   - 任务切分 StageScheduler  JobScheduler 排序 ExecutorScheduler任务分配
2. Executor
   - 执行计算
   - 为RDD 提供内存式存储，RDD直接缓存在executor进程内
3. 支持DAG 有向无环图 （宽依赖为划分阶段的依据）
4. task为多线程计每个Executor启动一个jvm效率更高（Hadoop 每个task为进程）

#####     Spark调优：

1. 性能调优
   - 最优资源配置 executor 数量，分配cpu核数，内存大小（提高task并行度，更高的缓存、shuffle内存）
   - RDD优化  复用 持久化 尽早filter
   - Kroy序列化 :默认java序列化为重量级序列化
   - spark本地化
2. 算子调优
   - map/foreachPartition  批量操作
   - filter和coalesce 共用
   - repartition 提高并行度
   - reduceByKey  会对本地数据进行combine  减少IO
3. shuffle优化
   - 调剂map端缓冲区大小 spark.shuffle.file.buffer 默认32k
   - 调节reduce端拉取数据缓冲区 spark.reducer.maxSizeInFligh 默认 48M
   - 调节入reduce端拉取重试次数 默认3
   - 调节reduce端拉取数据等待间隔 默认5s
   - 调节sortshuffle排序操作阈值 默认200
4. JVM调优
   - 静态/统一内存管理机制 storage：RDD和broadcast数据 Execution：shuffle产生的中间数据
   - 调节Executor堆外内存
   - 调节连接等待时长  SparkGC会导致Executor进程停止工作，无法建立网络连接，即超时

#### HBase 非关系型数据库，存储在HDFS

##### 结构

1. 逻辑结构：rowKey, column, columnFimaly, region, store 
2. 物理结构:storeFile(cell(.....timeStamp,type))
3. 基本架构
   - RegionServer  DML 管理Region
   - Master  DDL 管理RegionServer
   - Zookeeper 靠可用监控，集群维护
   - StoreFile：保存实际数据，以HFile形式存在HDFS
   - MemStore：写缓存HFile要求数据有序，数据在MemStore中排序，等到刷写时机形成StoreFile
   - WAl：预写日志，出现故障时可以依据此恢复
   - BlockCache：读缓存，保存高频的查询结果
   - HDFS 文件存储位置

##### 进阶

######     MemStore flush 刷写时机

1. **单个MemStroe** 

   ​	**刷写条件**：某个memstroe的大小达到了`hbase.hregion.memstore.flush.size（默认值128M`，其所在region的所有memstore都会刷写。

   ​	**无法写入**：（四倍flush.size）`hbase.hregion.memstore.flush.size（默认值128M）* hbase.hregion.memstore.block.multiplier（默认值4）`会阻止继续往该memstore写数据。

2. **全部MemStore**

   ​	 **刷写条件**: 当region server中memstore的总大小达到

   ```
   java_heapsize  --jvm堆内存*0.4*0.95 开始刷写
   *hbase.regionserver.global.memstore.size（默认值0.4）
   *hbase.regionserver.global.memstore.size.upper.limit（默认值upper = lower），
   *hbase.regionserver.global.memstore.size.lower.limit（默认值0.95）
   ```

   region会按照其所有memstore的大小顺序（由大到小）依次进行刷写。直到region server中所有memstore的总大小减小到上述值以下。
        **无法写入：** region server中memstore的总大小达到

   ```
   java_heapsize  jvm堆内存*0.4阻止写入
   *hbase.regionserver.global.memstore.size（默认值0.4）
   ```

   时，会阻止继续往所有的memstore写数据。

   ​    **自动刷写的时间**，也会触发memstore flush。自动刷新的时间间隔由该属性进行配置

   ```
     hbase.regionserver.optionalcacheflushinterval（默认1小时）。
   ```

   ​        最后一条写入数据时间开始计算

   ​    **WAL文件的数量**：超过`hbase.regionserver.max.logs`，region会按照时间顺序依次进行刷写，直到WAL文件数量减小到`hbase.regionserver.max.log`以下（该属性名已经废弃，现无需手动设置，最大值为32）。

######     StoreFile Compaction 合并

​		1.Minor Compaction 小合并只将小文件合并不删除数据

​		2.Major Compaction 大合并删除重复数据

######     Region Split

1. ##### （0.94版本之前）

   ​	当1个region中的某个Store下所有StoreFile的总大小超过hbase.hregion.max.filesize，该Region就会进行拆分。

2. #####  （0.94版本之后）

   ​	当1个region中的某个Store下所有StoreFile的总大小超过`Min(initialSize*R^3 ,hbase.hregion.max.filesize")`   该Region就会进行拆分。

   ​		    其中initialSize的默认值为`2*hbase.hregion.memstore.flush.size`，

   ​		    R为当前Region Server中属于该Table的Region个数（0.94版本之后）。

   ​					具体的切分策略为：

   ​					第一次split：1^3 * 256 = 256MB 

   ​					第二次split：2^3 * 256 = 2048MB 

   ​					第三次split：3^3 * 256 = 6912MB 

   ​					第四次split：4^3 * 256 = 16384MB > 10GB，因此取较小的值10GB 

   ​					后面每次split的size都是10GB了。

3. ##### Hbase 2.0

   ​	引入了新的split策略：如果当前RegionServer上该表只有一个Region，按照

   ​		`2 * hbase.hregion.memstore.flush.size`分裂，

   ​		否则按照`hbase.hregion.max.filesize`分裂。

##### 优化

1. 预分区
2. RowKey优化：防止数据倾斜
3. 内存优化：HBase 需要大量的内存，不建议分配过大内存GC 时间太久会导致RegionServer长期处于不可用状态 
4. 基础优化

##### Phoeix

实质为HBase的皮肤，以空间换时间，创建二级索引实质为，见了张新表



#### Redis NoSQL数据库 

- 用途：配合关系型数据库做高速缓存  高频词、热门访问的数据、降低 数据库IO

- Redis五大数据类型Key+ （String，List，Set， Hash，ZSet）

- Redis事务 采用乐观锁

- 主从复制 读写分离性能拓展，容灾快速恢复

- 哨兵模式  启动多个sentinel节点监控master

  - 主管下线：一台节点与master通讯中断
  - 客观下线：与master通讯中断节点达到预设数

- Redis持久化

  - RDB 将一段时间间隔内存中的数据集快照的方式写入磁盘（二进制文件）

    优点：节省磁盘空间,回复速度快

    缺点：出现故障会丢失一段时间的数据

  - AOF：以日志的形式记录每个写操作，记录所有执行过的指令

    优点：里理论上数据不会丢失

    缺点：比RDB更占磁盘空间

    ​			恢复备份速度慢

    ​		    每次读写都要同步，有一定性能压力

