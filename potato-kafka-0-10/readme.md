# potato-kafka  

## 简介  
kafka插件，提供source、sink与offsets管理工具。  

## 类说明  
* spark.potato.kafka.offsets.cmd.OffsetsCmd  
offsets命令行管理工具。  
* spark.potato.kafka.offsets.listener.OffsetsUpdateListener  
StreamingContext自动提交offsets的监听器。  
* spark.potato.kafka.offsets.manager.OffsetsManager  
offsets管理工具。  
* spark.potato.kafka.offsets.storage.OffsetsStorage  
offsets存储特质，目前支持原生kafka_broker/kafka_zookeeper与外部hbase实现。  
* spark.potato.kafka.sink._  
提供RDD[ProducerRecord[K, V]]与DStream[ProducerRecord[K, V]]写入hbase的能力。  
* spark.potato.kafka.sink.GlobalProducerCache  
KafkaProducer缓存池。  
* spark.potato.kafka.source._  
提供快速获取KafkaDirectStream的能力，并集成了OffsetsManager。  
* spark.potato.kafka.utils.OffsetsUtil  
offsets管理工具类。  

## 配置  
```properties
################################################################
# potato kafka config                                         #
################################################################
# kafka consumer 参数。
spark.potato.kafka.consumer.conf.group.id=potato_group
spark.potato.kafka.consumer.conf.auto.offset.reset=largest
spark.potato.kafka.consumer.conf.bootstrap.servers=test01:9092
# kafka producer 参数。
spark.potato.kafka.producer.conf.bootstrap.servers=test01:9092
spark.potato.kafka.producer.conf.key.serializer=org.apache.kafka.common.serialization.StringSerializer
spark.potato.kafka.producer.conf.value.serializer=org.apache.kafka.common.serialization.StringSerializer
# kafka source 订阅topic。
spark.potato.kafka.source.subscribe.topics=test1,test2
# offsets 存储类型。
spark.potato.kafka.offsets.storage=kafka
# offsets是否自动提交。
spark.potato.kafka.offsets.auto.update=true
# offsets自动提交延迟。
spark.potato.kafka.offsets.auto.update.delay=0
# hbase offsets存储表名。
spark.potato.kafka.offsets.storage.hbase.table=kafka_offsets_storage
# hbase offsets存储列族。
spark.potato.kafka.offsets.storage.hbase.family=partition
# hbase offsets地址参数。
spark.potato.kafka.offsets.storage.hbase.conf.hbase.zookeeper.quorum=test01
spark.potato.kafka.offsets.storage.hbase.conf.hbase.zookeeper.property.clientPort=2181
```