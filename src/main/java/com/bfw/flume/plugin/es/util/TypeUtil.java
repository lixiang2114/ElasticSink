package com.bfw.flume.plugin.es.util;

import java.lang.reflect.Array;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.regex.Pattern;

/**
 * @author Louis(LiXiang)
 * @description 简单类型字串转换工具
 */
@SuppressWarnings({ "unchecked" })
public class TypeUtil {
	/**
	 * 基本类型到包装类型映射字典
	 */
	public static final HashMap<Class<?>,Class<?>> BASE_TO_WRAP;
	
	/**
     * 逗号正则式
     */
	public static final Pattern COMMA_SEPARATOR=Pattern.compile(",");
	
	/**
     * 数字正则式
     */
	public static final Pattern NUMBER_CHARACTER=Pattern.compile("[0-9.]+");
    
	static{
		BASE_TO_WRAP=new HashMap<Class<?>,Class<?>>();
		BASE_TO_WRAP.put(byte.class, Byte.class);
		BASE_TO_WRAP.put(short.class, Short.class);
		BASE_TO_WRAP.put(int.class, Integer.class);
		BASE_TO_WRAP.put(long.class, Long.class);
		BASE_TO_WRAP.put(float.class, Float.class);
		BASE_TO_WRAP.put(double.class, Double.class);
		BASE_TO_WRAP.put(boolean.class, Boolean.class);
		BASE_TO_WRAP.put(char.class, Character.class);
		BASE_TO_WRAP.put(void.class, Void.class);
	}
	
	/**
	 * 字串转换为整数
	 * @param value 源值
	 * @return 转换值
	 */
	public static final Byte toByte(String value){
		return toType(value,Byte.class);
	}
	
	/**
	 * 字串转换为整数
	 * @param value 源值
	 * @return 转换值
	 */
	public static final Short toShort(String value){
		return toType(value,Short.class);
	}
	
	/**
	 * 字串转换为整数
	 * @param value 源值
	 * @return 转换值
	 */
	public static final Integer toInt(String value){
		return toType(value,Integer.class);
	}
	
	/**
	 * 字串转换为整数
	 * @param value 源值
	 * @return 转换值
	 */
	public static final Long toLong(String value){
		return toType(value,Long.class);
	}
	
	/**
	 * 字串转换为实数
	 * @param value 源值
	 * @return 转换值
	 */
	public static final Float toFloat(String value){
		return toType(value,Float.class);
	}
	
	/**
	 * 字串转换为实数
	 * @param value 源值
	 * @return 转换值
	 */
	public static final Double toDouble(String value){
		return toType(value,Double.class);
	}
	
	/**
	 * 字串转换为逻辑值
	 * @param value 源值
	 * @return 转换值
	 */
	public static final Boolean toBoolean(String value){
		return toType(value,Boolean.class);
	}
	
	/**
	 * 字串转换为字符值
	 * @param value 源值
	 * @return 转换值
	 */
	public static final Character toChar(String value){
		return toType(value,Character.class);
	}
	
	/**
	 * 字串转换为时间
	 * @param value 源值
	 * @return 转换值
	 */
	public static final Time toTime(String value){
		return toType(value,Time.class);
	}
	
	/**
	 * 字串转换为时间戳
	 * @param value 源值
	 * @return 转换值
	 */
	public static final Timestamp toTimestamp(String value){
		return toType(value,Timestamp.class);
	}
	
	/**
	 * 字串转换为通用日期
	 * @param value 源值
	 * @return 转换值
	 */
	public static final Date toDate(String value){
		return toType(value,Date.class);
	}
	
	/**
	 * 字串转换为SQL日期
	 * @param value 源值
	 * @return 转换值
	 */
	public static final java.sql.Date toSqlDate(String value){
		return toType(value,java.sql.Date.class);
	}
	
	/**
	 * 字串转换为通用日历
	 * @param value 源值
	 * @return 转换值
	 */
	public static final Calendar toCalendar(String value){
		return toType(value,Calendar.class);
	}
	
	/**
     * 指定类型是否为基本类型
     * @param type 类型
     * @return 是否为8种基本类型
     */
    public static final boolean isBaseType(Class<?> type){
    	return type.isPrimitive();
    }
    
    /**
     * 指定类型是否为包装类型
     * @param type 类型
     * @return 是否为8种包装类型
     */
    public static final boolean isWrapType(Class<?> type){
    	return BASE_TO_WRAP.containsValue(type);
    }
    
    /**
     * 指定类型是否为日期类型
     * @param type 类型
     * @return 是否为日期类型
     */
    public static final boolean isDateType(Class<?> type){
    	return Date.class.isAssignableFrom(type) || Calendar.class.isAssignableFrom(type);
    }
    
    /**
     * 指定类型是否为简单类型
     * @param type 类型
     * @return 是否为三类简单类型
     * @description
     * 简单类型包括8中基本类型、8种包装类型、字符串类型和日期类型
     */
    public static final boolean isSimpleType(Class<?> type){
    	if(isBaseType(type) || isWrapType(type) || String.class.isAssignableFrom(type)) return true;
    	if(Date.class.isAssignableFrom(type) || Calendar.class.isAssignableFrom(type)) return true;
    	return false;
    }
    
    /**
     * 指定类型是否为数字类型
     * @param type 类型
     * @return 是否为数字类型
     */
    public static final boolean isNumber(Class<?> type){
    	if(Number.class.isAssignableFrom(type)) return true;
    	if(boolean.class==type || char.class==type || void.class==type) return false;
    	return BASE_TO_WRAP.containsKey(type);
    }
    
    /**
     * 指定字串是否为纯数字串
     * @param type 类型
     * @return 是否为纯数字串
     */
    public static boolean isNumber(String string){
    	if(null==string || 0==string.trim().length()) return false;
    	return NUMBER_CHARACTER.matcher(string).matches();
    }
    
    /**
	 * 获取类型可能对应的包装类型
	 * @param type 参考类型
	 * @return 包装类型
	 */
	public static final Class<?> getWrapType(Class<?> type){
		if(null==type) return null;
		if(!type.isPrimitive()) return type;
		return BASE_TO_WRAP.get(type);
	}
    
    /**
	 * 判断给定的基类型superType是否可以兼容到指定的子类型childType
	 * @param superType 基类型
	 * @param childType 子类型
	 * @return 是否兼容
	 */
	public static final boolean compatible(Class<?> superType,Class<?> childType){
		if(superType==childType) return true;
		if(null==superType && null!=childType) return false;
		if(null!=superType && null==childType) return false;
		if(superType.isAssignableFrom(childType)) return true;
		try{
			if(superType.isPrimitive() && superType==childType.getField("TYPE").get(null)) return true;
			if(childType.isPrimitive() && childType==superType.getField("TYPE").get(null)) return true;
			return false;
		}catch(Exception e){
			return false;
		}
	}

	/**
	 * 通用简单类型转换
	 * @param value 源值
	 * @param R 目标类型
	 * @return 转换值
	 */
	public static final <R> R toType(String value,Class<R> returnType){
		if(null==value || null==returnType) return null;
		
		value=value.trim();
		if(returnType.isAssignableFrom(String.class)) return (R)value;
		if(0==value.length()) return null;
		
		if(Date.class.isAssignableFrom(returnType)){
			return (R)DateUtil.stringToDate(value,(Class<? extends Date>)returnType);
		}else if(Calendar.class.isAssignableFrom(returnType)){
			return (R)DateUtil.stringToCalendar(value);
		}else if(isNumber(returnType)){
			if(!NUMBER_CHARACTER.matcher(value).matches()) return null;
			Class<?> wrapType=getWrapType(returnType);
    		try {
                return (R)wrapType.getConstructor(String.class).newInstance(value);
            } catch (Exception e) {
                e.printStackTrace();
                return null;
            }
        }else if(Boolean.class==returnType || boolean.class==returnType){
            return (R)Boolean.valueOf(value);
        }else if(Character.class==returnType || char.class==returnType){
            return (R)Character.valueOf(value.charAt(0));
        }else if(returnType.isArray()){
        	String[] array=COMMA_SEPARATOR.split(value);
        	Class<?> componentType=returnType.getComponentType();
        	Object newArray=Array.newInstance(componentType, array.length);
        	for(int i=0;i<array.length;Array.set(newArray, i, toType(array[i].trim(),componentType)),i++);
        	return (R)newArray;
        }else{
        	throw new RuntimeException("java.lang.String Can Not Transfer To "+returnType.getName());
        }
	}
}
