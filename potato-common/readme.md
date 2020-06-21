# potato-common  

## 简介  
包含spark通用工具类，以及部分公用特质。  

## 类说明
* spark.potato.common.cmd.ActionCMDBase(Deprecated)  
快速创建可执行命令行类的特质，已废弃。  
* spark.potato.common.cmd.CmdParserUtil  
命令行参数解析工具。  
* potato.common.cmd.CommonCliBase  
快速创建可执行命令行类的特质。  
* potato.common.conf.PropertiesImplicits  
提供SparkConf与Properties类的转换。  
* potato.common.pool.KeyedCacheBase  
提供轻量级对象的缓存的特质，通过key进行存取，适合不频繁创建的对象。  
* potato.common.sender.Sender  
对外部系统发送消息的特质。  
* spark.potato.common.tools.DaemonThreadFactory    
生成守护线程的工厂对象。  
* potato.common.utils.JVMCleanUtil  
快速注册清理方法的工具类。  
* spark.potato.common.util.DingRobotUtil  
钉钉机器人发送信息工具类。  

## 配置  
```text
################################################################
# common config                                                #
################################################################
# 钉钉机器人token。
spark.potato.common.sender.ding.token=abc
# 钉钉机器人at人员，支持all/none/phone1,phone2。
spark.potato.common.sender.ding.at=none
```