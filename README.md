# spark_potato

## 简介  
spark开发插件，将常用到的功能进行封装，以便于快速开发。  

## 模块说明  
具体模块使用方式，请查看模块内的readme文档。  

简略说明见下:    
* potato-common  
    各种通用公共类、特质。 
* potato-hadoop  
    Configuration的序列化工具和小文件合并工具。  
* potato-hive  
    Hive数据export工具，目前支持export到kafka。  
* potato-hbase  
    sink与table访问工具。
* potato-kafka08(废弃)  
    基于旧版本api的source、sink与offsets管理工具。  
* potato-kafka010  
    基于新版本api的source、sink与offsets管理工具。  
* potato-quickstart  
    作业打包与命令行管理工具。  
* potato-spark  
    spark相关插件，包含模板、单例锁、流处理监控等插件。  

## 开发实例  
### 准备工作  
1. 下载源码并解压。  
2. 执行`./install.sh install`安装项目到本地maven仓库。  
3. 执行`./install.sh create [dir]`通过quickstart骨架创建项目到指定目录，或者在IDE里通过quickstart骨架直接创建项目。  

### 代码开发    
#### 批处理  
代码示例
```text
package potato.demo.batch

import potato.spark.template._

object BatchDemo extends FullTemplate {
  override def main(args: Array[String]): Unit = {
    val sc = createSC().withService.stopWhenShutdown
    println(sc.makeRDD(0 until 10).sum())
  }
}
```  

配置示例  
```text
################################################################
# 注意！所有非 spark. 前缀的参数，均不会被SparkConf加载。            #
# 如需添加自定义参数后在程序中调用，请注意此规则。                    #
################################################################
#
#
################################################################
# spark config                                                 #
################################################################
# 作业名称。
spark.app.name=BatchDemoTest
# 指向提交作业使用的主类。
spark.potato.main.class=potato.demo.batch.BatchDemo
#spark.potato.main.jar=potato-test-1.0-SNAPSHOT.jar
# streaming context批处理时间。
spark.potato.spark.streaming.batch.duration.ms=5000
# 需要开启附加服务，配置值为服务名称，如不开启附加服务，请删除此参数或配置为'false'
spark.potato.spark.additional.services=ContextSingletonLock
# 需要开启自定义服务，配置值为类全限定名，如不开启附加服务，请删除此参数或配置为'false'
#spark.potato.spark.custom.services.class=class.of.A,class.of.B
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
################################################################
# spark resource config                                        #
################################################################
spark.master=yarn
spark.submit.deployMode=client
spark.driver.cores=1
spark.driver.memory=512m
spark.executor.cores=2
spark.executor.memory=512m
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
## receiver背压。在kafka源下不生效。
#spark.streaming.backpressure.enabled=true
#spark.streaming.backpressure.initialRate
#spark.streaming.stopGracefullyOnShutdown
spark.yarn.driver.memoryOverhead=512m
spark.yarn.executor.memoryOverhead=512m
spark.yarn.queue=default
spark.yarn.submit.waitAppCompletion=true
```

### 部署
使用quickstart骨架，以fat包为例。  
执行`mvn clean package -DskipTests`生成tar包，注意调过测试。  
将tar包传入集群并解压，执行`./bin/potato submit -p conf/potato/demo/batch/BatchDemo.properties`提交作业。  
