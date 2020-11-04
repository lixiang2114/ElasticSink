package com.bfw.flume.plugin;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.lang.reflect.Method;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.BatchSizeSupported;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.ParseException;
import org.apache.http.RequestLine;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

import com.bfw.flume.plugin.util.ClassLoaderUtil;

/**
 * @author Louis(LiXiang)
 * @description Elastic-Sink插件
 */
@SuppressWarnings({"unchecked","unused"})
public class ElasticSink extends AbstractSink implements Configurable, BatchSizeSupported{
	/**
	 * 批处理尺寸
	 */
	private static int batchSize;
	
	/**
	 * 集群名称
	 */
	private static String clusterName;
	
	/**
	 * 主机列表
	 */
	private static HttpHost[] hostList;
	
	/**
	 * ES集群客户端
	 */
	private static RestClient restClient;
	
	/**
	 * 过滤方法
	 */
	private static Method doFilter;
	
	/**
	 * 文档ID名方法
	 */
	private static Method getDocId;
	
	/**
	 * 过滤器对象
	 */
	private static Object filterObject;
	
	/**
	 * 索引类型方法
	 */
	private static Method getIndexType;
	
	/**
	 * 索引名方法
	 */
	private static Method getIndexName;
	
	/**
	 * JSON工具
	 */
	private static final ObjectMapper MAPPER=new ObjectMapper();
	
	/**
	 * 
	 * 英文冒号正则式
	 */
	private static final Pattern COLON_REGEX=Pattern.compile(":");
	
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
	
	/**
	 * Sink默认过滤器
	 */
	private static final String DEFAULT_FILTER="com.bfw.flume.plugin.filter.impl.DefaultSinkFilter";
	
	/**
	 *  (non-Javadoc)
	 * @see org.apache.flume.sink.AbstractSink#start()
	 */
	@Override
	public synchronized void start() {
		super.start();
		RestClientBuilder builder=RestClient.builder(hostList);
		builder.setDefaultHeaders(new Header[]{new BasicHeader("Content-Type","application/json;charset=UTF-8")});
		restClient=builder.build();
	}

	/**
	 *  (non-Javadoc)
	 * @see org.apache.flume.sink.AbstractSink#stop()
	 */
	@Override
	public synchronized void stop() {
		try {
			if(null!=restClient) restClient.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		super.stop();
	}

	/**
	 *  (non-Javadoc)
	 * @see org.apache.flume.Sink#process()
	 */
	@Override
	public Status process() throws EventDeliveryException {
		if(null==filterObject||null==doFilter||null==getDocId||null==getIndexType||null==getIndexName){
			throw new RuntimeException("Error: Filter Not Initialized,Filter Object Is NULL...");
		}
		
		Status status=Status.READY;
		Channel channel=getChannel();
		Transaction tx=channel.getTransaction();
		
		tx.begin();
		try{
			for(int i=0;i<batchSize;i++){
				Event event=channel.take();
				if(null==event){
					status=Status.BACKOFF;
					break;
				}
				
				String record=new String(event.getBody(),Charset.defaultCharset()).trim();
				if(0==record.length()) continue;
				
				String indexNameStr=(String)getIndexName.invoke(filterObject);
				if(null==indexNameStr) continue;
				
				String indexName=indexNameStr.trim();
				if(0==indexName.length()) continue;
				
				String indexTypeStr=(String)getIndexType.invoke(filterObject);
				if(null==indexTypeStr) continue;
				
				String indexType=indexTypeStr.trim();
				if(0==indexType.length()) continue;
				
				Map<String,String> doc=(Map<String,String>)doFilter.invoke(filterObject, record);
				if(null==doc || 0==doc.size()) continue;
				
				String docIdStr=(String)getDocId.invoke(filterObject);
				String docId=null==docIdStr?"":docIdStr.trim();
				String docIdVal=null;
				
				if(0!=docId.length() && 0!=(docIdVal=doc.getOrDefault(docId, "").trim()).length()){
					push(indexName,indexType,docIdVal,doc);
				}else{
					pushByType(indexName,indexType,doc);
				}
			}
			
			tx.commit();
			return status;
		}catch(Throwable e){
		  tx.rollback();
		  return Status.BACKOFF;
		}finally{
			tx.close();
		}
	}

	/**
	 *  (non-Javadoc)
	 * @see org.apache.flume.conf.BatchSizeSupported#getBatchSize()
	 */
	@Override
	public long getBatchSize() {
		return batchSize;
	}

	/**
	 *  (non-Javadoc)
	 * @see org.apache.flume.conf.Configurable#configure(org.apache.flume.Context)
	 */
	@Override
	public void configure(Context context) {
		//装载自定义过滤器类路径
		try {
			addFilterClassPath();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		
		//获取自定义的过滤器类
		Class<?> filterType=null;
		try {
			filterType = getFilterClass();
		} catch (ClassNotFoundException | IOException e) {
			throw new RuntimeException(e);
		}
		
		if(null==filterType) throw new RuntimeException("Not Found Filter Class...");
		
		//初始化过滤器对象与接口表
		try {
			filterObject=filterType.newInstance();
			getDocId=filterType.getDeclaredMethod("getDocId");
			doFilter=filterType.getDeclaredMethod("doFilter",String.class);
			getIndexType=filterType.getDeclaredMethod("getIndexType");
			getIndexName=filterType.getDeclaredMethod("getIndexName");
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		
		//初始化上下文参数
		clusterName=getParamValue(context,"clusterName", "ES-Cluster");
		batchSize=Integer.parseInt(getParamValue(context,"batchSize", "100"));
		String[] hosts=COMMA_REGEX.split(getParamValue(context,"hostList", "127.0.0.1:9200"));
		hostList=new HttpHost[hosts.length];
		for(int i=0;i<hosts.length;i++){
			String host=hosts[i].trim();
			if(0==host.length()) continue;
			String[] ipAndPort=COLON_REGEX.split(host);
			if(ipAndPort.length>=2){
				String ip=ipAndPort[0].trim();
				String port=ipAndPort[1].trim();
				if(!IP_REGEX.matcher(ip).matches()) continue;
				if(!NUMBER_REGEX.matcher(port).matches()) continue;
				hostList[i]=new HttpHost(ip, Integer.parseInt(port), "http");
				continue;
			}
			
			if(ipAndPort.length<=0) continue;
			
			String unknow=ipAndPort[0].trim();
			if(NUMBER_REGEX.matcher(unknow).matches()){
				hostList[i]=new HttpHost("127.0.0.1", Integer.parseInt(unknow), "http");
			}else if(IP_REGEX.matcher(unknow).matches()){
				hostList[i]=new HttpHost(unknow, 9200, "http");
			}
		}
		
		//初始化自定义参数
		Method contextConfig=null;
		try {
			contextConfig = filterType.getDeclaredMethod("contextConfig",Map.class);
		} catch (NoSuchMethodException | SecurityException e1) {}
		
		if(null==contextConfig) {
			System.out.println("Warn: the filter may not be initialized,"+filterType.getName());
			return;
		}
		
		try {
			contextConfig.invoke(filterObject, context.getParameters());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 获取过滤器类
	 * @return 过滤类
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 */
	private static final Class<?> getFilterClass() throws IOException, ClassNotFoundException {
		InputStream inStream=ClassLoaderUtil.getClassPathFileStream("filter.properties");
		Properties properties=new Properties();
		properties.load(inStream);
		String filterClass=properties.getProperty(ElasticSink.class.getName(),DEFAULT_FILTER).trim();
		if(0==filterClass.length()) filterClass=DEFAULT_FILTER;
		return Class.forName(filterClass);
	}
	
	/**
	 * 添加过滤器类路径
	 * @throws URISyntaxException
	 * @throws IOException 
	 */
	private static final void addFilterClassPath() throws URISyntaxException, IOException{
		File file = new File(new File(ElasticSink.class.getResource("/").toURI()).getParentFile(),"filter");
		if(!file.exists()) file.mkdirs();
		
		File configFile=new File(file,"filter.properties");
		if(!configFile.exists()) configFile.createNewFile();
		
		ClassLoaderUtil.addFileToCurrentClassPath(file, ElasticSink.class);
	}
	
	/**
	 * 获取参数值
	 * @param context Sink插件上下文
	 * @param key 参数名
	 * @param defaultValue 默认参数值
	 * @return 参数值
	 */
	private static final String getParamValue(Context context,String key,String defaultValue){
		String value=context.getString(key,defaultValue).trim();
		return value.length()==0?defaultValue:value;
	}
	
	/**
	 * 提交文档对象到ES服务
	 * @param indexName 索引名称
	 * @param document 文档对象 
	 */
	private static final void push(String indexName,Object document) {
		push(indexName,null,null,document);
	}
	
	/**
	 * 提交文档对象到ES服务
	 * @param indexName 索引名称
	 * @param indexType 索引类型
	 * @param docId 文档ID
	 * @param document 文档对象 
	 */
	private static final void pushById(String indexName,String docId,Object document) {
		push(indexName,null,docId,document);
	}
	
	/**
	 * 提交文档对象到ES服务
	 * @param indexName 索引名称
	 * @param indexType 索引类型
	 * @param document 文档对象 
	 */
	private static final void pushByType(String indexName,String indexType,Object document) {
		push(indexName,indexType,null,document);
	}
	
	/**
	 * 提交文档对象到ES服务
	 * @param indexName 索引名称
	 * @param indexType 索引类型
	 * @param docId 文档ID
	 * @param document 文档对象 
	 */
	private static final void push(String indexName,String indexType,String docId,Object document) {
		if(null==indexName || 0==indexName.trim().length()) return;
		if(null==indexType || 0==indexType.trim().length()) indexType="_doc";
		
		StringBuilder uriBuilder=new StringBuilder("/");
		uriBuilder.append(indexName.trim()).append("/");
		uriBuilder.append(indexType.trim());
		
		if(null!=docId && 0!=docId.trim().length()) uriBuilder.append("/").append(docId.trim());
		uriBuilder.append("?pretty");
		
		executePost(uriBuilder.toString(),document,new BasicHeader("Content-Type","application/json;charset=UTF-8"));
	}
	
	/**
	 * 发起HTTP请求
	 * @param uri 请求路径
	 * @param queryString 查询字串
	 * @param headers 头域列表
	 * @return 响应对象
	 */
	private static final Response executeGet(String uri,Map<String,String> queryString,Header... headers) {
		return executeRequest(uri,"GET",queryString,null,headers);
	}
	
	/**
	 * 发起HTTP请求
	 * @param uri 请求路径
	 * @param msgBody 消息体对象
	 * @param headers 头域列表
	 * @return 响应对象
	 */
	private static final Response executePost(String uri,Object msgBody,Header... headers) {
		return executeRequest(uri,"POST",null,msgBody,headers);
	}
	
	/**
	 * 发起HTTP请求
	 * @param uri 请求路径
	 * @param queryString 查询字串
	 * @param msgBody 消息体对象
	 * @param headers 头域列表
	 * @return 响应对象
	 */
	private static final Response executeRequest(String uri,Map<String,String> queryString,Object msgBody,Header... headers) {
		return executeRequest(uri,"POST",queryString,msgBody,headers);
	}
	
	/**
	 * 发起HTTP请求
	 * @param uri 请求路径
	 * @param method 请求方法
	 * @param queryString 查询字串
	 * @param msgBody 消息体对象
	 * @param headers 头域列表
	 * @return 响应对象
	 */
	private static final Response executeRequest(String uri,String method,Map<String,String> queryString,Object msgBody,Header... headers) {
		if(null==uri || 0==uri.trim().length()) return null;
		if(null==method || 0==method.trim().length()) method="POST";
		
		Request request=new Request(method,uri);
		if(null!=queryString && 0!=queryString.size()) request.addParameters(queryString);
		if(null!=headers && 0!=headers.length) addHeader(request,headers);
		
		HttpEntity msgEntity = null;
		if(null!=msgBody){
			StringWriter writer=new StringWriter();
			try {
				MAPPER.writeValue(writer, msgBody);
				msgEntity = new StringEntity(writer.toString(),ContentType.APPLICATION_JSON);
			} catch (IOException e1) {
				e1.printStackTrace();
			}
		}
		if(null!=msgEntity) request.setEntity(msgEntity);
		
		try {
			return restClient.performRequest(request);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	/**
	 * 添加请求头
	 * @param request 请求对象
	 * @param headers 头域列表
	 */
	private static final void addHeader(Request request,Header... headers) {
		RequestOptions.Builder builder=request.getOptions().toBuilder();
		for(Header header:headers) builder.addHeader(header.getName(), header.getValue());
		 request.setOptions(builder);
	}
	
	/**
	 * 打印REST响应结果
	 * @param response
	 * @throws ParseException
	 * @throws IOException
	 */
	private static void printResult(Response response) throws ParseException, IOException{
		RequestLine requestLine=response.getRequestLine();
		String contentTypeHeader=response.getHeader("content-type");
		String responseBody=EntityUtils.toString(response.getEntity());
		System.out.println("请求行:\n"+requestLine);
		System.out.println("响应消息类型:\n"+contentTypeHeader);
		System.out.println("响应消息体内容:\n"+responseBody);
	}
}
