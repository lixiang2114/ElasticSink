import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

import com.github.lixiang2114.flume.plugin.es.filter.ElasticSinkFilter;

public class ElasticLoggerFilter implements ElasticSinkFilter {
	
	private String[] fields;
	
	private String indexType;
	
	private String indexName;
	
	private String fieldSeparator;
	
	private static String userName;
	
	private static String passWord;
	
	private static Pattern commaRegex;

	public String getDocId() {
		return this.fields[0];
	}

	public String getPassword() {
		return passWord;
	}

	public String getUsername() {
		return userName;
	}

	public String getIndexType() {
		return this.indexType;
	}

	public String getIndexName() {
		return this.indexName;
	}

	public Map<String, Object> doFilter(String record) {
		String[] fieldValues = commaRegex.split(record);
	    HashMap map = new HashMap();
	    map.put(this.fields[0], fieldValues[0].trim());
	    map.put(this.fields[1], fieldValues[1].trim());
	    map.put(this.fields[2], fieldValues[2].trim());
	    return map;
	}

	public void filterConfig(Properties properties) {
		commaRegex = Pattern.compile(this.fieldSeparator);
	}
}
