### 插件开发背景
ElasticSink是Flume流处理工具下基于Elasticsearch的一款Sink插件，由于Flume自带的Elasticsearch插件紧耦合到Elasticsearch的发行版本中，当Elasticsearch版本升级后，Flume自带的Elasticsearch插件不能及时更新版本（截止目前Flume1.9版本的Elasticsearch插件还是基于Elasticsearch5.X版本的），这导致Flume自带的Elasticsearch插件无法连接到高版本的Elasticsearch服务

后期基于Transport的TCP客户端将逐渐被遗弃和取代，Elastic官方不再推荐使用，因此重写ElasicSink插件不可能再基于TCP协议，目前推荐的是使用基于REST风格的Elastic客户端构建应用程序，而Height Level Rest Client强依赖于Elasticsearch的发型版本并随同Elasticsearch版本同步发布，因此，如果基于Height Level Rest Client来构建Flume-Sink插件可能会出现与Flume自带Elastic插件一样的结果，故本插件基于Low Level Rest Client构建，构建时选用的版本为Low Level Rest Client的最高版本elasticsearch-rest-client-7.9.3


​      
​      
### ElasticSink插件特性
1. 版本无关性  
ElasticSink插件被设计成不依赖于任何Elasticsearch版本（即它与Elasticsearch版本无关），因为他是基于REST风格的HTTP协议实现，除了自身issue需要复验以外，不会因为任何版本问题导致其插件启动失败或是Elastic服务连接失效

2. 插件扩展性  
这是一款Flume-Sink插件，它除了基于默认配置来完成一些简单的基础过滤功能，还提供了基于JAVA语言自定义的过滤器扩展，使用者可以根据自己的业务定制编写自己的个性化过滤器并将其放置到Flume安装目录下的filter目录中，同时配置好使用自定义过滤器，该插件即可回调自定义过滤器完成日志记录的过滤操作
  
    
   
      
### 插件使用说明
#### Flume工具及插件安装
1. 下载JDK-1.8.271  
wget https://download.oracle.com/otn/java/jdk/8u271-b09/61ae65e088624f5aaa0b1d2d801acb16/jdk-8u271-linux-x64.tar.gz
  
    
   
2. 安装JDK-1.8.271  
tar -zxvf jdk-8u271-linux-x64.tar.gz -C /software/jdk1.8.0_271  
echo -e "JAVA_HOME=/software/jdk1.8.0_271\nPATH=$PATH:$JAVA_HOME/lib:$JAVA_HOME/bin\nexport PATH JAVA_HOME">>/etc/profile && source /etc/profile
  
    
   
3. 下载Flume-1.9.0  
wget https://github.com/lixiang2114/Software/raw/main/flume-1.9.0.zip
  
    
   
4. 安装Flume-1.9.0  
unzip flume-1.9.0.zip -d /software/
  
    
   
5. 下载插件ElasticSink-1.0  
wget https://github.com/lixiang2114/ElasticSink/raw/main/depends.zip
  
    
   
6. 安装插件ElasticSink-1.0  
unzip depends.zip   &&   cp -a depends/*   /software/flume-1.9.0/lib/  
  
    
   
      
#### Elasticsearch服务安装
1. 下载Elasticsearch  
wget https://github.com/lixiang2114/Software/raw/main/elasticsearch-6.8.8.zip  
  
    
   
2. 安装Elasticsearch  
useradd -lmd /home/elastic elastic  
unzip elasticsearch-6.8.8.zip -d /software/  
chown -R elastic:elastic /software/elasticsearch-6.8.8  
  
    
   

说明：    
若搭建ES集群，请修改各个物理节点上配置文件：/software/elasticsearch-6.8.8/config/elasticsearch.yml，将其中的cluster.name参数统一成一个名字（默认为elasticsearch）、各物理节点上Elastic例程的node.name参数值在同一个Elastic集群中必须保持唯一；同时结合官网给出的配置调整系统内核参数（如：文件描述符、系统软硬进程数、堆栈参数及CPU核心数等）


​      
​      

#### ElasticSink插件基础使用
**Note：**下面以抽取日志为例来说明插件的基本使用方法    

1. 编写Shell命令或脚本  
```Shell
vi /software/flume-1.9.0/process/script/getLogger.sh
#!/usr/bin/env bash
while true;do
    tailf -0 /install/test/mylogger.log 2>/dev/null
    sleep 1s
done

chmod a+x /software/flume-1.9.0/process/script/getLogger.sh
```


2. 编写Flume任务流程配置  
```Text
vi /software/flume-1.9.0/process/conf/example02.conf
a1.sources=s1
a1.sinks=k1 k2
a1.channels=c1 c2

a1.sources.s1.type=exec
a1.sources.s1.command=/software/flume-1.9.0/process/script/getLogger.sh
a1.sources.s1.batchSize=20
a1.sources.s1.batchTimeout=3000
a1.sources.s1.restart=true
a1.sources.s1.restartThrottle=10000
a1.sources.s1.channels=c1 c2
a1.sources.s1.selector.type=replicating

a1.sinks.k1.type=logger
a1.sinks.k1.channel=c1

a1.sinks.k2.type=com.bfw.flume.plugin.ElasticSink
a1.sinks.k2.hostList=192.168.162.129:9200
a1.sinks.k2.fieldList=times,level,message
a1.sinks.k2.clusterName=ES-Cluster
a1.sinks.k2.fieldSeparator=,
a1.sinks.k2.indexName=user
a1.sinks.k2.indexType=logger
a1.sinks.k2.docId=times
a1.sinks.k2.channel=c2

a1.channels.c1.type=memory
a1.channels.c1.capacity=1000
a1.channels.c1.transactionCapacity=100
a1.channels.c2.type=memory

a1.channels.c2.capacity=1000
a1.channels.c2.transactionCapacity=100
```


3. 启动Elastic服务  
```Shell
su -l elastic
/software/elasticsearch-6.8.8/sbin/ESTools start
lsof -i tcp:9200
lsof -i tcp:9300
```


4. 启动Flume服务  
```Shell
/software/flume-1.9.0/bin/flume-ng agent -c /software/flume-1.9.0/conf -f /software/flume-1.9.0/process/conf/example02.conf -n a1 -Dflume.root.logger=INFO,console
```


5. 使用Shell模拟日志产生以测试Flume插件  
```Shell
for index in {1..100000};do echo "${index},info,this is my ${index} times test";echo "${index},info,this is my ${index} times test">> /install/test/mylogger.log;sleep 0.001s;done
```


​      
​      

#### ElasticSink插件过滤器使用  
##### 过滤器接口规范简介
不同的Sink组件可以对应到不同的插件过滤器，编写插件过滤器的接口规范如下：  
```JAVA
package com.bfw.flume.plugin.filter;

import java.util.Map;

/**
 * @author Louis(LiXiang)
 * @description 自定义Sink过滤器接口规范
 */
public interface SinkFilter {
	/**
	 * 获取文档ID字段名
	 * @return ID字段名
	 */
	public String getDocId();
	
	/**
	 * 获取文档索引类型
	 * @return 索引类型
	 */
	public String getIndexType();
	
	/**
	 * 获取文档索引名称
	 * @return 索引名称
	 */
	public String getIndexName();
	
	/**
	 * 处理文档记录
	 * @param record 文本记录
	 * @return 文档字典对象
	 */
	public Map<String,String> doFilter(String record);
	
	/**
	 * 插件上下文配置(可选实现)
	 * @param config 配置
	 */
	default public void pluginConfig(Map<String,String> config){}
	
	/**
	 * 过滤器上下文配置(可选实现)
	 * @param config 配置
	 */
	default public void filterConfig(Properties properties){}
}
```
说明：  
编写插件过滤器通常需要实现SinkFilter接口，但这并不是必须的，考虑到程序员编码的灵活性，ElasticSink插件被设计成约定优于配置的原则，因此程序员只需要在自定义的过滤器实现类中提供相应的接口规范即可，ElasticSink总是可以根据接口规范检索到对应的接口签名并正确无误的去回调它   


​    
##### 自定义过滤器实现步骤  
1. 编写过滤器实现类  
```JAVA
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import com.bfw.flume.plugin.filter.SinkFilter;

/**
 * @author Louis(LiXiang)
 * @description 自定义日志过滤器
 */
public class LoggerFilter implements SinkFilter{
	/**
	 * 空白正则式
	 */
	private static final Pattern BLANK_REGEX=Pattern.compile("\\s+");
	
	/**
	 * 日期正则式
	 */
	private static final Pattern DATE_REGEX=Pattern.compile("^\\d{4}-\\d{2}-\\d{2}$");
	
	public LoggerFilter(){
		//公共无参构造是必须的，可以在此构造器中读取自定义配置文件中参数来初始化本类成员变量
	}

	@Override
	public String getDocId() {
		//返回文档对象ID字段名，若返回NULL或空白则由Elastic随机生成_id字段值
		
		return "userId"; 
	}

	@Override
	public String getIndexType() {
		//返回文档对象索引类型，若返回NULL或空白则不会被路由到Elastic服务
		
		return "logger"; 
	}

	@Override
	public String getIndexName() {
		//返回文档对象索引名称，若返回NULL或空白则不会被路由到Elastic服务
		
		return "user"; 
	}

	@Override
	public Map<String, String> doFilter(String record) { 
		//返回需要路由到Elastic服务的文档对象，若返回NULL或空字典则不会被路由到Elastic服务
		
		if(null==record) return null; //空日志无需收集
		
		String row=record.trim();
		if(0==row.length()) return null; //空日志无需收集
		
		String[] fieldValues=BLANK_REGEX.split(row);
		if(!DATE_REGEX.matcher(fieldValues[0]).matches()) return null; //日志不满足条件无需收集
		
		HashMap<String,String> map=new HashMap<String,String>(); //包装文档字典
		map.put("userId", fieldValues[2].trim());
		map.put("userName", fieldValues[3].trim());
		map.put("address", fieldValues[4].trim());
		
		return map; //返回文档字典
	}

	@Override
	public void contextConfig(Map<String, String> config) {
		//这里可以获取插件配置文件中参数值来初始化本类成员变量
	}
	
	@Override
	public void filterConfig(Properties properties) {
		//这里可以获取过滤器配置文件中参数值来初始化本类成员变量
	}
}
```

说明：  
可以使用Eclipse、Idea等IDE集成开发工具来完成上述编码和编译过程，如果过滤器项目是基于Maven构建的，还可以直接使用Maven来编译项目，如果过滤器简单到只有单个类文件也可以直接使用命令行编译：  
javac -cp ElasticSink-1.0.jar LoggerFilter.java  

如果编译后的项目不止一个字节码文件则需要打包：  
Maven： mvn package -f  /xxx/pom.xml  
JAVA：jar -cvf xxx.jar -C \[project\]  


​    
2. 发布过滤器  
* 发布过滤器代码  
不论过滤器项目编译后是单个字节码文件还是压缩打成的jar包，我们都可以直接将其拷贝到filter目录下的lib子目录中即可：  
cp -a LoggerFilter.class /software/flume-1.9.0/filter/lib/  
或  
cp -a LoggerFilter.jar /software/flume-1.9.0/filter/lib/  
  
    
  
* 配置发布的过滤器  
```Text
vi /software/flume-1.9.0/filter/filter.properties  
type=LoggerFilter
indexType=mylog
indexName=mytest
fieldSeparator=,
fields=docId,level,msg
```


​    
说明：  
因为上述的LoggerFilter非常简单，就是一个字节码文件，没有定义包名（即存在于类路径下的默认包中），所以看到的就是一个类名，如果过滤器的入口类（实现SinkFilter接口的类）有包名则必须带上包名  

经过以上步骤之后，我们启动Flume服务，其ElasticSink插件就会自动调动我们自定义的过滤器类LoggerFilter来完成日志过滤处理了  