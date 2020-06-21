# potato-kafka-0-10  

## 简介  
kafka插件，提供source、sink与offsets管理工具。  

## 类说明  
* potato.kafka010.conf.PotatoKafkaConf  
kafka配置解析工具，将SparkConf解析为consumer和producer可用的Properties。  
* potato.kafka010.offsets.cmd.KafkaOffsetCli  
offsets命令行管理工具。  
* potato.kafka010.offsets.listener.OffsetsUpdateListener    
StreamingContext自动提交offsets的监听器。  
* potato.kafka010.offsets.manager.OffsetsManager    
offsets管理工具。  
* potato.kafka010.offsets.storage.OffsetsStorage  
offsets存储特质，目前支持原生kafka_broker/kafka_zookeeper与外部hbase实现。  
* potato.kafka010.offsets.KafkaConsumerOffsetsUtil  
基于新SimpleConsumer的offsets管理工具。  
* potato.kafka010.offsets.SimpleConsumerOffsetsUtil  
基于旧SimpleConsumer的offsets管理工具。  
* potato.kafka010.sink  
提供RDD[ProducerRecord[K, V]]与DStream[ProducerRecord[K, V]]写入hbase的能力。  
* potato.kafka010.sink.GlobalProducerCache  
KafkaProducer缓存池。  
* potato.kafka010.sink.KafkaSinkUtil  
kafka sink工具类。  
* potato.kafka010.sink.SpeedLimitedProducer  
扩展KafkaProducer提供限速功能，限制每秒发送记录数，避免下游kafka性能不足导致数据丢失问题。
* potato.kafka010.source  
提供快速获取KafkaDirectStream的能力，并集成了OffsetsManager。  
* potato.kafka010.writer.KafkaWriter    
提供DataFrame写入kafka的能力。  

## 配置  
```text
################################################################
# 注意！所有非 spark. 前缀的参数，均不会被SparkConf加载。            #
# 如需添加自定义参数后在程序中调用，请注意此规则。                    #
################################################################
#
#
################################################################
# kafka config                                                 #
################################################################
## kafka源背压。
spark.streaming.kafka.maxRatePerPartition=1000
spark.streaming.kafka.maxRetries=1
# kafka common 参数。
spark.potato.kafka.common.bootstrap.servers=test01:9092,test02:9092,test03:9092
# kafka consumer 参数。
spark.potato.kafka.consumer.group.id=potato_group
spark.potato.kafka.consumer.auto.offset.reset=latest
spark.potato.kafka.consumer.key.deserializer=org.apache.kafka.common.serialization.StringDeserializer
spark.potato.kafka.consumer.value.deserializer=org.apache.kafka.common.serialization.StringDeserializer
# kafka producer 参数。
spark.potato.kafka.producer.key.serializer=org.apache.kafka.common.serialization.StringSerializer
spark.potato.kafka.producer.value.serializer=org.apache.kafka.common.serialization.StringSerializer
# kafka source 订阅topic。
spark.potato.kafka.source.subscribe.topics=test1,test2
# offsets 存储类型。
spark.potato.kafka.offsets.storage.type=kafka
# offsets是否自动提交。
spark.potato.kafka.offsets.storage.auto.update=true
# offsets自动提交延迟。
spark.potato.kafka.offsets.storage.update.delay=0
# hbase offsets存储表名。
spark.potato.kafka.offsets.storage.hbase.table=kafka_offsets_storage
# hbase offsets存储列族。
spark.potato.kafka.offsets.storage.hbase.family=partition
# hbase offsets地址参数。
spark.potato.kafka.offsets.storage.hbase.conf.hbase.zookeeper.quorum=test01
spark.potato.kafka.offsets.storage.hbase.conf.hbase.zookeeper.property.clientPort=2181
```