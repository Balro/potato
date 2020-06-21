# potato-quickstart  

## 简介  
spark快速开发骨架，集成了命令行管理脚本。  

## 部署
quickstart骨架已集成maven-assembly插件，有default、fat、server三个profile。  
* default: 客户端模式，仅打包应用jar包，需要额外部署server包并配置POTATO_HOME变量。优点是包占用空间小。  
* fat: fatjar模式，将作业jar和依赖jar全部打包，包占用空间大。优点是可以独立部署，不会受其他作业共享包干扰。  
* server: 服务端模式，仅打包potato相关jar包，用于为default模式提供共享包支持，同时可以执行部分命令行管理脚本。  

## 脚本结构  
* bin/potato
入口脚本，提供对各子模块的调用支持。  
依赖SPARK_BIN于SPARK_ARGS变量。  
SPARK_BIN指向spark-submit位置，SPARK_ARGS用于追加spark-submit参数。    
    ```text
    Usage:
      potato [opts] <module> [module_args]
    
      opts:
        -h|--help     ->  show module usage.
        -v|--version  ->  show potato version.
    
      modules:
        submit        ->  submit app to spark.
        hadoop        ->  manage hadoop utils.
        hive          ->  manage hive utils.
        kafka         ->  manage kafka010 utils.
        spark         ->  manage spark utils.
    ```  
* bin/potato-client  
客户端入口脚本，至potato脚本的软链。  
依赖POTATO_HOME变量，变量指向server包位置，用于查找共享包。
* bin/modules/submit.sh  
用于spark作业提交。  
    ```text
    Usage:
      potato submit <opts> [main jar args]
    
    opts-required:
      -p|--prop-file       specify the property file to load.
    opts-optional:
      -c|--class <class>   main class on spark submit.
      -j|--jar <jar>       main jar file on spark submit.
      --conf <key=value>   additional spark conf.
    ```  
* bin/modules/hadoop.sh  
hadoop小文件合并工具入口。  
    ```text
    Usage:
      potato hadoop <opts> [main jar args]
    
    opts:
      --file-merge <args>  file merge function.
      --conf <key=value>   additional spark conf.
    ```  
* bin/modules/hive.sh  
hive数据导出工具入口。
    ```text
    Usage:
      potato hive <opts> [main jar args]
    
    opts:
      --export <args>      export hive data.
      --conf <key=value>   additional spark conf.
    ```  
* bin/modules/kafka.sh  
kafka offsets管理工具入口。  
    ```text
    Usage:
      potato kafka <opts> [main jar args]
    
    opts:
      --offset <args>      manage the kafka offset.
      --conf <key=value>   additional spark conf.
    ```
* bin/modules/spark.sh  
spark单例锁管理工具入口。  
    ```text
    Usage:
      potato spark <opts> [main jar args]
    
    opts:
      --lock <args>      manage the spark lock.
      --conf <key=value>   additional spark conf.
    ```  
