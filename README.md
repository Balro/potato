# spark_streaming_potato

## 目的
通过对 spark_streaming 的常用插件进行封装，简化开发流程，提高开发效率。  
该项目提供的各个插件，彼此之间尽可能简化依赖，使其可以单独在其他任何一个 
spark_streaming 项目中使用。  

## 模块
* potato-common  
    包含供模块的公共类，util类等。
* potato-plugins  
    spark与其他组件的集成插件，提供多组件访问功能。  
* potato-template  
    作业模板，预先集成部分插件，同时供开发参考用。  
* potato-quickstart  
    开发原型，可通过原型快速构建骨架，同时包含作业管理脚本。
