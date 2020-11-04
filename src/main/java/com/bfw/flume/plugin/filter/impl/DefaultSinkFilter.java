package com.bfw.flume.plugin.filter.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import com.bfw.flume.plugin.filter.SinkFilter;

/**
 * @author Louis(LiXiang)
 * @description Elastic-Sink默认过滤器
 */
public class DefaultSinkFilter implements SinkFilter{
	/**
	 * 文档ID字段名
	 */
	private static  String docId;
	
	/**
	 * 记录字段列表
	 * 按记录行从左到右区分顺序
	 */
	private static String[] fieldList;
	
	/**
	 * 索引类型
	 */
	private static String indexType;
	
	/**
	 * 索引名称
	 */
	private static String indexName;
	
	/**
	 * 记录字段默认分隔符为中英文空白正则式
	 */
	private static Pattern fieldSeparator;
	
	/**
	 * 英文逗号正则式
	 */
	private static final Pattern COMMA_REGEX=Pattern.compile(",");
	
	/**
     * 数字正则式
     */
	public static final Pattern NUMBER_REGEX=Pattern.compile("^[0-9]+$");
	
	/**
     * IP地址正则式
     */
	public static final Pattern IP_REGEX=Pattern.compile("^\\d+\\.\\d+\\.\\d+\\.\\d+$");

	@Override
	public String getDocId() {
		return DefaultSinkFilter.docId;
	}

	@Override
	public String getIndexType() {
		return DefaultSinkFilter.indexType;
	}

	@Override
	public String getIndexName() {
		return DefaultSinkFilter.indexName;
	}
	
	public static void setDocId(String docId) {
		DefaultSinkFilter.docId = docId;
	}

	public static void setFieldList(String[] fieldList) {
		DefaultSinkFilter.fieldList = fieldList;
	}

	public static void setIndexType(String indexType) {
		DefaultSinkFilter.indexType = indexType;
	}

	public static void setIndexName(String indexName) {
		DefaultSinkFilter.indexName = indexName;
	}

	public static void setFieldSeparator(Pattern fieldSeparator) {
		DefaultSinkFilter.fieldSeparator = fieldSeparator;
	}

	@Override
	public Map<String, String> doFilter(String record) {
		HashMap<String,String> doc=new HashMap<String,String>();
		String[] fieldValues=fieldSeparator.split(record);
		if(null==fieldList || 0==fieldList.length){
			for(int i=0;i<fieldValues.length;doc.put("field"+i, fieldValues[i]),i++);
		}else if(fieldList.length>=fieldValues.length){
			for(int i=0;i<fieldValues.length;doc.put(fieldList[i], fieldValues[i]),i++);
		}else{
			int i=0;
			for(;i<fieldList.length;doc.put(fieldList[i], fieldValues[i]),i++);
			for(;i<fieldValues.length;doc.put("field"+i, fieldValues[i]),i++);
		}
		return doc;
	}

	@Override
	public void contextConfig(Map<String, String> config) {
		docId=getParamValue(config,"docId", "docId");
		indexType=getParamValue(config,"indexType", "logger");
		indexName=getParamValue(config,"indexName", "fpdata");
		fieldSeparator=Pattern.compile(getParamValue(config,"fieldSeparator","\\s+"));
		
		String fieldListStr=getParamValue(config,"fieldList",null);
		if(null!=fieldListStr){
			String[] fields=COMMA_REGEX.split(fieldListStr);
			fieldList=new String[fields.length];
			for(int i=0;i<fields.length;i++){
				String fieldName=fields[i].trim();
				if(0==fieldName.length()){
					fieldList[i]="field"+i;
					continue;
				}
				fieldList[i]=fieldName;
			}
		}
		
	}
	
	/**
	 * 获取参数值
	 * @param context Sink插件上下文
	 * @param key 参数名
	 * @param defaultValue 默认参数值
	 * @return 参数值
	 */
	private static final String getParamValue(Map<String, String> context,String key,String defaultValue){
		String value=context.getOrDefault(key, defaultValue).trim();
		return value.length()==0?defaultValue:value;
	}
}
