package com.bfw.flume.plugin.es.filter;

import java.util.Map;
import java.util.Properties;

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
	public Map<String,Object> doFilter(String record);
	
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
