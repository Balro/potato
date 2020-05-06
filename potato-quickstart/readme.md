# potato-quickstart  

## 简介  
spark作业命令行管理工具，可以通过命令行方便地对作业进行管理并使用potato插件功能。  

## 脚本说明  
* bin/potato.sh  
```shell script
# 入口脚本，提供对各插件模块的访问功能。
Usage:
  potato.sh <opts> -- [module args]

  opts:
    -h,--help   <module>   ->  show module usage
    -m,--module <module>   ->  module to be launched
    -p,--prop <prop_file>  ->  properties file for spark-submit

  modules:
    submit   ->  submit app to cluster.
    lock     ->  manage app lock.
    offsets  ->  manage kafka offsets.
```  
* bin/exec/lock.sh  
```shell script
# RunningLock管理模块。
Usage:
  potato.sh -p <potato_conf_file> -m lock <args>
  args:
    clear -> clear old lock to stop app.
    state -> show lock status.
```  
* bin/exec/offsets.sh  
```shell script
# KafkaOffsets管理模块。
Usage:
  potato.sh -p <potato_conf_file> -m offsets <args>
  args:
    list  -> clear old lock to stop app.
    lag   -> show current lag.
    reset -> reset offsets to earliest or latest.
```  
* bin/exec/submit.sh  
```shell script
# Spark作业提交模块。
Usage:
  $(basename "$0") -p <potato_conf_file> -m submit [-s] [main jar args]
Args:
  -s  start app silently and log msg to logfile.
```  

# 配置  
```properties
################################################################
# 注意！所有非 spark. 前缀的参数，均不会被SparkConf加载。           #
# 如需添加自定义参数后在程序中调用，请注意此规则。                    #
################################################################
#
#
################################################################
# potato submit config                                         #
################################################################
# spark-submit脚本，用于在某些集群中区别spark1和spark2，比如cdh。
spark.potato.submit.bin=spark2-submit
# todo 重要参数！主类入口。
spark.potato.submit.main.class=quickstart.xxx
spark.potato.submit.main.jar=potato-quickstart-0.1.1-SNAPSHOT.jar
#
#
################################################################
# spark config                                                 #
################################################################
# todo 重要参数！作业名称，用于运行锁的标识，必须唯一。
spark.app.name=potato_quickstart_demo
spark.master=yarn
spark.submit.deployMode=cluster
spark.driver.cores=1
spark.driver.memory=512m
spark.executor.cores=2
spark.executor.memory=512m
## 作业调度方式，支持 FIFO(默认) 和 FAIR 。
# spark.scheduler.mode=FIFO
## 启用dynamicAllocation。
spark.shuffle.service.enabled=true
spark.dynamicAllocation.enabled=true
spark.dynamicAllocation.executorIdleTimeout=60s
spark.dynamicAllocation.cachedExecutorIdleTimeout=1h
spark.dynamicAllocation.initialExecutors=1
spark.dynamicAllocation.maxExecutors=2
spark.dynamicAllocation.minExecutors=1
spark.dynamicAllocation.schedulerBacklogTimeout=5s
spark.dynamicAllocation.sustainedSchedulerBacklogTimeout=5s
## 仅在staticAllocation模式生效。
# spark.executor.instances=2
## classpath参数，仅在cluster模式生效。
spark.driver.userClassPathFirst=false
spark.executor.userClassPathFirst=false
## 开启背压。
spark.streaming.backpressure.enabled=true
#spark.streaming.backpressure.initialRate
#spark.streaming.stopGracefullyOnShutdown
#spark.streaming.kafka.maxRatePerPartition=1000
#spark.streaming.kafka.maxRetries=1
#
#
################################################################
# yarn config                                                  #
################################################################
spark.yarn.driver.memoryOverhead=512m
spark.yarn.executor.memoryOverhead=512m
spark.yarn.queue=default
spark.yarn.submit.waitAppCompletion=false
#
#
################################################################
# potato common config                                         #
################################################################
# todo 重要参数！streaming context批处理时间。
spark.potato.common.streaming.batch.duration.ms=5000
# 附加服务列表，全限定类名。如不开启附加服务，请删除此参数或配置为'false'。
spark.potato.common.additional.services=spark.potato.lock.singleton.SingletonLockManager,spark.potato.monitor.backlog.BacklogMonitorService
#
#
################################################################
# potato hbase config                                         #
################################################################
# hbase zookeeper 地址。
spark.potato.hbase.conf.hbase.zookeeper.quorum=localhost
# hbase zookeeper 端口。
spark.potato.hbase.conf.hbase.zookeeper.property.clientPort=2181
#
#
################################################################
# potato kafka config                                         #
################################################################
# kafka consumer 参数。
# todo 重要参数！存储kafka的groupId，必须唯一。
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
#
#
################################################################
# potato lock config                                           #
################################################################
# 获取锁最大重试次数。
spark.potato.lock.running.try.max=3
# 获取锁重试间隔。
spark.potato.lock.running.try.interval.ms=30000
# 是否强制获取锁，如配置true，则会清楚旧锁。
spark.potato.lock.running.force=true
# 锁心跳间隔。
spark.potato.lock.running.heartbeat.interval.ms=10000
# 锁心跳超时时间。
spark.potato.lock.running.heartbeat.timeout.ms=90000
# 锁存储类型。
spark.potato.lock.running.type=zookeeper
# zookeeper锁地址。
spark.potato.lock.running.zookeeper.quorum=test01:2181
# zookeeper锁路径。
spark.potato.lock.running.zookeeper.path=/potato/lock/running
#
#
################################################################
# potato monitor config                                        #
################################################################
# 批次积压告警阈值。
spark.potato.monitor.backlog.delay.ms=60000
# 批次积压告警间隔。
spark.potato.monitor.backlog.reporter.interval.ms=600000
# 批次积压告警最大次数。
spark.potato.monitor.backlog.reporter.max=3
# 批次积压告警类型。
spark.potato.monitor.backlog.reporter.type=ding
# 钉钉告警token。
spark.potato.monitor.backlog.reporter.ding.token=xxx
# 钉钉告警是否at所有人。
spark.potato.monitor.backlog.reporter.ding.at.all=false
# 钉钉告警需要at的手机号列表。
spark.potato.monitor.backlog.reporter.ding.at.phones=123,456
```  
