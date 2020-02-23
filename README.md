# spark_streaming_potato

## 简介  
一个简单易用的 spark_streaming 开发脚手架工具。

## 开发目的  
通过使用该脚手架或插件，简化开发过程。  

## 模块说明  
* potato-common  
    包含供模块的公共类，util类等。
* potato-plugins  
    spark与其他组件的集成插件，提供多组件访问功能。  
* potato-template  
    作业模板，预先集成部分插件，同时可供开发参考。  
* potato-quickstart  
    开发骨架，可快速构建项目，同时提供作业管理脚本。  
    
## 使用说明  
目前脚手架只支持spark-yarn-cluster部署模式。具体使用方式请参考wiki。  
https://github.com/Balro/spark_streaming_potato/wiki

## 参考与感谢
感谢 fire-spark 项目提供的灵感。
