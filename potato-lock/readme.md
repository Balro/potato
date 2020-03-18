# potato-lock  

## 简介  
通过分布式锁实现spark作业的单例执行和远程停止功能。  

## 类说明  
* spark.potato.lock.running.RunningLock  
RunningLock特质，目前为zookeeper实现。  
* spark.potato.lock.running.RunningLockCmd  
RunningLock命令行管理工具。  
* spark.potato.lock.running.RunningLockManager  
集成spark的管理工具，请对SparkContext和StreamingContext分别使用ContextRunningLockService与StreamingRunningLockService。  

# 配置  
```properties
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
```  
