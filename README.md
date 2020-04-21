# spark_streaming_potato

## 简介  
spark开发插件，包含多种组件的source、sink模块，和作业管理模块。  

## 模块说明  
具体模块使用方式，请查看模块内的readme文档。  

简略说明见下:    
* potato-common  
    各种通用公共类、特质。
* potato-deploy  
    用于部署potato通用jar包，并且提供工具脚本。  
* potato-hadoop  
    Configuration的序列化类。  
* potato-hbase  
    sink与table访问工具。
* potato-kafka  
    source、sink与offsets管理工具。  
* potato-lock  
    sc与ssc管理工具，提供分布式锁与心跳功能。  
* potato-monitor  
    streaming批次积压检测上报工具。  
* potato-quickstart  
    作业打包与命令行管理工具。  
* potato-template  
    作业模板，提供开发参考。  

## 快速开始  
### 准备工作  
1. 下载源码并解压。  
2. 执行`./install.sh install`安装项目到本地maven仓库。  
3. 执行`./install.sh create [dir]`通过quickstart骨架创建项目到指定目录，或者在IDE里通过quickstart骨架直接创建项目。  

### 开始开发    
#### 批处理  
代码示例
```scala
package spark.potato.quickstart.batch

import spark.potato.template.batch.BatchTemplate

object BatchDemo extends BatchTemplate {
  override def doWork(): Unit = {
    val sc = createContext()
    println(sc.makeRDD(0 until 10).sum())
  }
}
```  
配置示例
```properties
################################################################
# potato submit config                                         #
################################################################
# spark-submit脚本，用于在某些集群中区别spark1和spark2，比如cdh。
spark.potato.submit.bin=spark2-submit
# todo 重要参数！主类入口。
spark.potato.submit.main.class=spark.potato.quickstart.batch.BatchDemo
spark.potato.submit.main.jar=potato-quickstart-0.1.1-SNAPSHOT.jar
################################################################
# spark config                                                 #
################################################################
# todo 重要参数！作业名称，用于运行锁的标识，必须唯一。
spark.app.name=BatchDemo
spark.master=yarn
spark.submit.deployMode=client
# ...
################################################################
# potato common config                                         #
################################################################
# todo 重要参数！streaming context批处理时间。
spark.potato.common.streaming.batch.duration.ms=5000
# 附加服务列表，全限定类名。如不开启附加服务，请删除此参数或配置为'false'。
spark.potato.common.additional.services=spark.potato.lock.running.ContextRunningLockService
# ...
################################################################
# potato lock config                                           #
################################################################
# ...
# zookeeper锁地址。
spark.potato.lock.running.zookeeper.quorum=test01:2181
# zookeeper锁路径。
spark.potato.lock.running.zookeeper.path=/potato/lock/running
```  
#### 流处理  
代码示例  
```scala
package spark.potato.quickstart.streaming

import java.util.Date
import java.util.concurrent.TimeUnit

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import spark.potato.common.context.StreamingContextUtil
import spark.potato.template.streaming.StreamingTemplate

import scala.collection.mutable

object StreamingDemo extends StreamingTemplate {
  private val queue = mutable.Queue.empty[RDD[String]]

  override def doWork(): Unit = {
    val ssc = createStreamingContext()
    val stream = ssc.queueStream(queue)
    stream.print()
    start(ssc)
  }

  override def afterStart(ssc: StreamingContext): Unit = {
    while (!ssc.sparkContext.isStopped) {
      queue += ssc.sparkContext.makeRDD(Seq("test data: " + new Date().toString))
      TimeUnit.MILLISECONDS.sleep(StreamingContextUtil.getBatchDuration(ssc).milliseconds)
    }
  }
}
```  
配置示例  
```properties
################################################################
# potato submit config                                         #
################################################################
# spark-submit脚本，用于在某些集群中区别spark1和spark2，比如cdh。
spark.potato.submit.bin=spark2-submit
# todo 重要参数！主类入口。
spark.potato.submit.main.class=spark.potato.quickstart.streaming.StreamingDemo
spark.potato.submit.main.jar=potato-quickstart-0.1.1-SNAPSHOT.jar
################################################################
# spark config                                                 #
################################################################
# todo 重要参数！作业名称，用于运行锁的标识，必须唯一。
spark.app.name=StreamingDemo
spark.master=yarn
spark.submit.deployMode=cluster
# ...
################################################################
# potato common config                                         #
################################################################
# todo 重要参数！streaming context批处理时间。
spark.potato.common.streaming.batch.duration.ms=5000
# 附加服务列表，全限定类名。如不开启附加服务，请删除此参数或配置为'false'。
spark.potato.common.additional.services=spark.potato.lock.running.StreamingRunningLockService,spark.potato.monitor.backlog.BacklogMonitorService
# ...
################################################################
# potato lock config                                           #
################################################################
# ...
# zookeeper锁地址。
spark.potato.lock.running.zookeeper.quorum=test01:2181
# zookeeper锁路径。
spark.potato.lock.running.zookeeper.path=/potato/lock/running
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
### 部署
quickstart骨架已集成maven-assembly插件，直接执行`mvn clean package`即可打包tar包。  
将tar包传入集群并解压，执行`./bin/potato.sh -p [prop_file] -m submit`即可提交作业。  
已配置RunningLock的作业，可以通过`./bin/potato.sh -p [prop_file] -m lock -- clear`远程停止作业。

## 注意  
个人项目不可避免存在存在bug，使用前请先对代码进行测试，如发现bug望及时反馈。
