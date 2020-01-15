# spark_streaming_potato

## 一句话简介  
一个简单易用的 spark_streaming 开发脚手架工具。

## 项目开发目的  
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
请参考wiki。  
https://github.com/Balro/spark_streaming_potato/wiki

## coder自述
coder在工作过程中认识到，模块化、标准化很重要。为了锻炼自己的能力，同时产出一些"有价值"的东西，
便决心开发一套属于自己的 spark_streaming 脚手架服务。  
该服务的设计思想，在于将开发过程中，常用到的代码进行模块化，封装后统一管理。在开发时，需要用到的
插件直接通过 maven 导入。同时，针对常用的开发流程设计通用模板，使得开发人员可以将精力更多得投入
在业务逻辑而非重复的 SparkContext 创建上。  
该项目目前还在初级阶段，存在插件不全，逻辑bug等诸多问题。coder会持续进行更新和完善，让项目
一起 "grow" 起来。

## 参考与感谢
感谢 GuoNingNing 前辈的 fire-spark 项目。
