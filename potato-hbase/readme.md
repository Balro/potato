# potato-hbase  
## 简介  
hbase集成插件，提供连接缓存，table缓存，source与sink等工具。  

## 类说明  
* spark.potato.hbase.util.HBaseConfigurationUtil  
从SparkConf中提取指定前缀的参数并加载为Configuration。  
* spark.potato.hbase.connection.GlobalConnectionCache  
jvm级别的连接缓存池，通过spark.potato.hbase.connection.ConnectionInfo进行存取。  
* spark.potato.hbase.sink._  
hbase_sink插件，提供对RDD[MutationAction]与DStream[MutationAction]写入hbase的能力。  
* spark.potato.hbase.table.SinkTable  
写入hbase的sink_table特质，目前有BufferedSinkTable实现。  
* spark.potato.hbase.util.TableUtil  
快速访问hbase的工具类。  

## 配置
```text
################################################################
# hbase config                                                 #
################################################################
# hbase zookeeper 地址。
spark.potato.hbase.conf.hbase.zookeeper.quorum=localhost
# hbase zookeeper 端口。
spark.potato.hbase.conf.hbase.zookeeper.property.clientPort=2181
```  
