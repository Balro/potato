# potato-monitor  

## 简介  
spark监控插件，目前仅支持StreamingContext。  

## 类说明  
* spark.potato.monitor.backlog.BacklogMonitorService  
集成StreamingContext监控积压批次的工具类。  
* spark.potato.monitor.reporter.Reporter  
用于上报监控信息的特质，目前实现为Ding机器人。  

## 配置  
```properties
################################################################
# potato monitor config                                           #
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
