# potato-common  

## 简介  
包含spark通用工具类，以及部分公用特质。  

## 类说明
* spark.potato.common.cache.KeyedCacheBase  
提供轻量级对象的缓存的特质，通过key进行存取，适合不频繁创建的对象。  
* spark.potato.common.cmd.ActionCMDBase  
快速创建可执行命令行类的特质。  
* spark.potato.common.cmd.CmdParserUtil  
命令行参数解析工具。  
* spark.potato.common.context.StreamingContextUtil  
获取StreamingContext批处理间隔的工具类。  
* spark.potato.common.service._  
提供了spark添加附加服务的特质。  
* spark.potato.common.tools.DaemonThreadFactory    
生成守护线程的工厂对象。  
* spark.potato.common.util.CleanUtil  
快速注册清理方法的工具类。  
* spark.potato.common.util.ContextUtil  
为spark提供自动停止方法的工具类。  
* spark.potato.common.util.DingRobotUtil  
钉钉机器人发送信息工具类。  
* spark.potato.common.util.LocalLauncherUtil  
本地测试spark作业工具类。  

## 配置  
```properties
################################################################
# potato common config                                         #
################################################################
# 作业名称。
spark.app.name=test
# streaming context批处理时间。
spark.potato.common.streaming.batch.duration.ms=5000
# 需要开启附加服务，配置值为类全限定名，如不开启附加服务，请删除此参数或配置为'false'
spark.potato.common.additional.services=class.of.A,class.of.B
```