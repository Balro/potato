# potato-spark  
## 简介  
spark集成插件。  

## 类说明  
* potato.spark.conf.SparkConfUtil    
SparkConf加载工具。  
* potato.spark.lock.singleton  
spark单例锁工具。  
* potato.spark.monitor.streaming  
spark streaming积压信息上报监听器。  
* potato.spark.service  
spark注册服务管理插件。  
* potato.spark.sql.writer  
DataFrame落地插件。  
* potato.spark.streaming  
SparkStreamingContext管理工具。  
* potato.spark.template  
快速开发方法模板。  
* potato.spark.util  
杂项工具。  

## 配置文件
```text
################################################################
# spark config                                                 #
################################################################
# 作业名称。
spark.app.name=test
spark.potato.main.class=Test
# streaming context批处理时间。
spark.potato.spark.streaming.batch.duration.ms=5000
# 需要开启附加服务，配置值为服务名称，如不开启附加服务，请删除此参数或配置为'false'
spark.potato.spark.additional.services=name1,name2
# 需要开启自定义服务，配置值为类全限定名，如不开启附加服务，请删除此参数或配置为'false'
spark.potato.spark.custom.services.class=class.of.A,class.of.B
### 锁配置。
# 获取锁最大重试次数。
spark.potato.lock.singleton.try.max=3
# 获取锁重试间隔。
spark.potato.lock.singleton.try.interval.ms=30000
# 是否强制获取锁，如配置true，则会清楚旧锁。
spark.potato.lock.singleton.force=true
# 锁心跳间隔。
spark.potato.lock.singleton.heartbeat.interval.ms=10000
# 锁心跳超时时间。
spark.potato.lock.singleton.heartbeat.timeout.ms=90000
# 锁存储类型。
spark.potato.lock.singleton.type=zookeeper
# zookeeper锁地址。
spark.potato.lock.singleton.zookeeper.quorum=test01:2181
# zookeeper锁路径。
spark.potato.lock.singleton.zookeeper.path=/potato/spark/lock/singleton
### 监控配置。
# 批次积压告警阈值。
spark.potato.spark.monitor.streaming.backlog.threshold.ms=60000
# 批次积压告警间隔。
spark.potato.spark.monitor.streaming.backlog.report.interval.ms=600000
# 批次积压告警最大次数。
spark.potato.spark.monitor.streaming.backlog.report.max=3
# 批次积压告警类型。
spark.potato.spark.monitor.streaming.backlog.report.sender=ding
```
