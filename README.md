# ElasticSink
ElasticSink是Flume流处理工具下基于Elasticsearch的一款Sink插件，由于Flume自带的Elasticsearch插件紧耦合到Elasticsearch的发行版本中，当Elasticsearch升级版本后，Flume自带的Elasticsearch插件不能及时更新（截止目前Flume1.9版本的Elasticsearch插件还是基于Elasticsearch5.X版本的），这导致Flume自带的Elasticsearch插件无法连接到Elasticsearch服务

后期基于Transport的TCP客户端将被逐渐遗弃，不再推荐使用，因此重写的ElasicSink插件不可能基于TCP协议，目前推荐的是使用Rest风格的客户端构建应用，而Height Level Rest Client强依赖于Elasticsearch的发型版本并随同Elasticsearch版本同步发布，因此，如果基于Height Level Rest Client来构建插件可能出现与Flume自带Elastic插件一样的结果，故本插件基于Elasticsearch Low Level Rest Client构建，构建时选用的版本为Low Level Rest Client最高版本elasticsearch-rest-client-7.9.3
