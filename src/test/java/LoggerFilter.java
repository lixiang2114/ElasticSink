import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

import com.bfw.flume.plugin.es.filter.SinkFilter;

/**
 * @author Louis(LiXiang)
 * @description 自定义日志过滤器
 */
public class LoggerFilter implements SinkFilter{
	/**
	 * 字段列表
	 */
	private String[] fields;
	
	/**
	 * 文档索引类型
	 */
	private String indexType;
	
	/**
	 * 文档索引名称
	 */
	private String indexName;
	
	/**
	 * 日志记录字段分隔符
	 */
	private String fieldSeparator;
	
	/**
	 * 逗号正则式
	 */
	private static Pattern commaRegex;
	
	@Override
	public String getDocId() {
		return fields[0]; 
	}

	@Override
	public String getIndexType() {
		return indexType; 
	}

	@Override
	public String getIndexName() {
		return indexName; 
	}

	@Override
	public Map<String, Object> doFilter(String record) { 
		String[] fieldValues=commaRegex.split(record);
		HashMap<String,Object> map=new HashMap<String,Object>();
		map.put(fields[0], fieldValues[0].trim());
		map.put(fields[1], fieldValues[1].trim());
		map.put(fields[2], fieldValues[2].trim());
		return map;
	}

	@Override
	public void filterConfig(Properties properties) {
		commaRegex=Pattern.compile(fieldSeparator);
	}
}
