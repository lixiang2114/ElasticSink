package com.github.lixiang2114.flume.plugin.es.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;

/**
 * @author Louis(LiXiang)
 * @description 通用类装载器工具,用例如下:
 * 获取当前类路径列表
 * URL[] urls=ClassLoaderUtil.getCurrentClassPath();
 * 
 * 添加目录到类路径
 * ClassLoaderUtil.addFileToCurrentClassPath("D:/conf");
 * Class<?> type=ClassLoaderUtil.loadType("com.thoughtworks.xstream.XStream");
 * Object object=type.newInstance();
 * 
 * InputStream fis=ClassLoaderUtil.getClassPathFileStream("my.properties");
 * Properties pro=new Properties();
 * pro.load(fis);
 * 
 * 将指定的jar文件装载到类路径
 * ClassLoaderUtil.addFileToCurrentClassPath("d:/conf/xstream-all-1.4.7.jar");
 * Object object=ClassLoaderUtil.instanceClass("com.thoughtworks.xstream.XStream");
 * 
 * 获取指定jar文件的类装载器
 * ClassLoader classLoader=ClassLoaderUtil.getFileClassLoader("d:/conf/xstream-all-1.4.7.jar");
 * Class<?> type=classLoader.loadClass("com.thoughtworks.xstream.XStream");
 * Object object=type.newInstance();
 * System.out.println(object);
 * 
 * 获取指定jar目录的类装载器
 * ClassLoader classLoader=ClassLoaderUtil.getPathClassLoader("d:/conf");
 * Class<?> type=classLoader.loadClass("com.thoughtworks.xstream.XStream");
 * Object object=type.newInstance();
 * System.out.println(object);
 * 
 * 获取指定jar目录的类装载器
 * ClassLoader classLoader=ClassLoaderUtil.getPathClassLoader("d:/conf");
 * Class<?> type=ClassLoaderUtil.loadType("com.thoughtworks.xstream.XStream", classLoader);
 * Object object=type.newInstance();
 * System.out.println(object);
 * 
 * 获取指定jar目录的类装载器
 * ClassLoader classLoader=ClassLoaderUtil.getPathClassLoader("d:/conf");
 * Object object=ClassLoaderUtil.instanceClass("com.thoughtworks.xstream.XStream", Object.class, classLoader);
 * System.out.println(object);
 * 
 * 获取指定绝对路径中的某个类
 * Class<?> type=ClassLoaderUtil.getClass("d:/conf","com.thoughtworks.xstream.XStream");
 * Object object=type.newInstance();
 * System.out.println(object);
 * 
 * 获取指定绝对路径中某个类的实例
 * Object object=ClassLoaderUtil.getInstance("d:/conf","com.thoughtworks.xstream.XStream");
 * System.out.println(object);
 * 
 * 获取指定绝对路径文件中的某个类
 * Class<?> type=ClassLoaderUtil.getClass("d:/conf/xstream-all-1.4.7.jar","com.thoughtworks.xstream.XStream");
 * Object object=type.newInstance();
 * System.out.println(object);
 * 
 * 获取指定绝对路径文件中某个类的实例
 * Object object=ClassLoaderUtil.getInstance("d:/conf/xstream-all-1.4.7.jar","com.thoughtworks.xstream.XStream");
 * System.out.println(object);
 * 
 * 装载类到方法区
 * Class<?> type=ClassLoaderUtil.loadType("java.util.List");
 * System.out.println(null==type);
 * 
 * 判断类是否存在
 * boolean exists=ClassLoaderUtil.existClass("java.util.ArrayList");
 * 
 * #装载并实例化类
 * ClassLoader classLoader=ClassLoaderUtil.getCurrentClassLoader();
 * Class<?> cla=classLoader.loadClass("java.util.ArrayList");
 * List list=(List)cla.newInstance();
 * list.add("louis");
 * list.add(38);
 * System.out.println(list);
 * 
 * 直接实例化类
 * List list=ClassLoaderUtil.instanceClass("java.util.ArrayList",List.class);
 * list.add("louis");
 * list.add(38);
 * System.out.println(list);
 */
public class ClassLoaderUtil{
	/**
	 * 添加文件到类路径方法
	 */
	private static Method addClassPathMethod;
	
	static{
		try {
			addClassPathMethod = URLClassLoader.class.getDeclaredMethod("addURL", URL.class);
			addClassPathMethod.setAccessible(true);
		} catch (NoSuchMethodException | SecurityException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 校正类路径
	 * @param classPath 类路径
	 * @return 类路径
	 */
	public static String fixClassPath(String classPath){
		if(null==classPath || classPath.trim().isEmpty()) return null;
		String classPathFile=classPath.trim();
		if(classPathFile.startsWith("\\")){
			classPathFile="/"+classPathFile.substring(1);
		}else if(classPathFile.startsWith("./")||classPathFile.startsWith(".\\")){
			classPathFile="/"+classPathFile.substring(2);
		}else if(!classPathFile.startsWith("/")){
			classPathFile="/"+classPathFile;
		}
		return classPathFile;
	}
	
	/**
	 * 获取指定绝对路径文件的输入流
	 * @param absolutePathfile 绝对路径文件
	 * @return 指向绝对路径文件的输入流
	 */
	public static InputStream getAbsolutePathFileStream(String absolutePathfile){
		File file=new File(absolutePathfile);
		if(!file.exists()||file.isDirectory()) return null;
		try {
			return file.toURI().toURL().openStream();
		} catch (Exception e) {
			return null;
		}
	}
	
	/**
	 * 获取类文件对应的真实文件
	 * @param classPath 类路径文件
	 * @return 指向绝对路径的真实文件
	 */
	public static File getRealFile(String classPath){
		String fileFullPath=getRealPath(classPath);
		if(null==fileFullPath) return null;
		return new File(fileFullPath);
	}
	
	/**
	 * 获取类文件对应的真实路径
	 * @param classPath 类路径文件
	 * @return 指向绝对路径的真实文件
	 */
	public static String getRealPath(String classPath){
		String fileClassPath=fixClassPath(classPath);
		if(null==fileClassPath) return null;
		Class<?> caller=getCallerClass();
		if(null==caller) return null;
		return caller.getResource(fileClassPath).getPath();
	}
	
	/**
	 * 获取指定类文件的输入流
	 * @param classPathfile 类路径文件
	 * @return 指向类路径文件的输入流
	 */
	public static InputStream getClassPathFileStream(String classPathfile){
		String fileClassPath=fixClassPath(classPathfile);
		if(null==fileClassPath) return null;
		Class<?> caller=getCallerClass();
		if(null==caller) return null;
		return caller.getResourceAsStream(fileClassPath);
	}
	
	/**
	 * 获取当前类的类装载器(该方法通常在本类的外部调用)
	 * @return 类装载器
	 */
	public static ClassLoader getCurrentClassLoader(){
		return getCallerClassLoader();
	}
	
	/**
	 * 获取当前类的调用类的类装载器(该方法通常在本类的外部调用)
	 * @return 类装载器
	 */
	public static ClassLoader getCurrentCallerClassLoader(){
		Class<?> currentClass=getCallerClass();
		return getCallerClassLoader(currentClass);
	}
	
	/**
	 * 获取可选参数类的调用类
	 * @param currentClass 可选的基准参数类
	 * @return 类装载器
	 */
	public static ClassLoader getCallerClassLoader(Class<?>... currentClass){
		Class<?> caller=getCallerClass(currentClass);
		return null==caller?null:caller.getClassLoader();
	}
	
	/**
	 * 获取可选参数类(默认为GenericClassLoader)的调用类
	 * @param currentClass 参数类(不能是Thread类)
	 * @return 调用者类(不能是Thread类和GenericClassLoader类)
	 */
	public static Class<?> getCallerClass(Class<?>... currentClass){
		Class<?> type=(null==currentClass||0==currentClass.length)?ClassLoaderUtil.class:currentClass[0];
		StackTraceElement[] elements=Thread.currentThread().getStackTrace();
		String currentClassName=type.getName();
		
		int startIndex=-1;
		for(int i=0;i<elements.length;i++){
			if(!currentClassName.equals(elements[i].getClassName())) continue;
			startIndex=i;
			break;
		}
		
		if(-1==startIndex) return null;
		
		String callerClassName=null;
		for(int i=startIndex+1;i<elements.length;i++){
			String iteClassName=elements[i].getClassName();
			if(currentClassName.equals(iteClassName)) continue;
			callerClassName=iteClassName;
			break;
		}
		
		if(null==callerClassName) return null;
		
		try {
			return Class.forName(callerClassName);
		} catch (ClassNotFoundException e) {
			return null;
		}
	}
	
	/**
	 * 获取当前类的类装载器的classpath中的路径集合
	 * @param type 参考类
	 * @return URL路径集合
	 * @throws IOException IO异常
	 */
	public static URL[] getCurrentClassPath(Class<?>... type) throws IOException{
		ClassLoader classLoader=getCallerClassLoader(type);
		if(null==classLoader) return null;
		return ((URLClassLoader)classLoader).getURLs();
	}
	
	/**
	 * 添加参数目录(目录或jar文件)到当前类路径
	 * @param fullPath 目录或jar文件的绝对路径
	 * @param type 类装载器的参考类
	 * @description 
	 * 装载指定的参数jar文件或从装载参数目录开始递归装载所有子目录及其jar文件
	 * 注意装载到类路径的都是目录而不是文件,而jar文件则被视为一种特殊的目录被装载到类路径,所以除jar文件以外的普通文件是不能被装载到类路径的
	 */
	public static void addFileToCurrentClassPath(String fullPath,Class<?>... type) {
		addFileToCurrentClassPath(new File(fullPath),type);
	}
	
	/**
	 * 添加参数目录(目录或jar文件)到当前类路径
	 * @param file 目录或jar文件的绝对路径
	 * @param type 类装载器的参考类
	 * @description 
	 * 装载指定的参数jar文件或从装载参数目录开始递归装载所有子目录及其jar文件
	 * 注意装载到类路径的都是目录而不是文件,而jar文件则被视为一种特殊的目录被装载到类路径,所以除jar文件以外的普通文件是不能被装载到类路径的
	 */
	public static void addFileToCurrentClassPath(URL url,Class<?>... type) {
		File file=null;
		try {
			file=new File(url.toURI());
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}
		if(null==file) return;
		addFileToCurrentClassPath(file,type);
	}
	
	/**
	 * 添加参数目录(目录或jar文件)到当前类路径
	 * @param file 目录或jar文件的绝对路径
	 * @param type 类装载器的参考类
	 * @description 
	 * 装载指定的参数jar文件或从装载参数目录开始递归装载所有子目录及其jar文件
	 * 注意装载到类路径的都是目录而不是文件,而jar文件则被视为一种特殊的目录被装载到类路径,所以除jar文件以外的普通文件是不能被装载到类路径的
	 */
	public static void addFileToCurrentClassPath(URI uri,Class<?>... type) {
		addFileToCurrentClassPath(new File(uri),type);
	}
	
	/**
	 * 添加参数目录(目录或jar文件)到当前类路径
	 * @param file 目录或jar文件的绝对路径
	 * @param type 类装载器的参考类
	 * @description 
	 * 装载指定的参数jar文件或从装载参数目录开始递归装载所有子目录及其jar文件
	 * 注意装载到类路径的都是目录而不是文件,而jar文件则被视为一种特殊的目录被装载到类路径,所以除jar文件以外的普通文件是不能被装载到类路径的
	 */
	public static void addFileToCurrentClassPath(File file,Class<?>... type) {
		if(!file.exists()) throw new RuntimeException(file.getAbsolutePath()+" is not exists...");
		
		ClassLoader classLoader=getCallerClassLoader(type);
		if(null==classLoader) throw new RuntimeException("can not get caller classloader...");
		
		addFileToCurrentClassPath(file,classLoader);
	}

	/**
	 * 添加参数目录(目录或jar文件)到当前类路径
	 * @param file 目录文件或jar文件
	 * @param classLoader 类装载器
	 * @description 装载指定的参数jar文件或从装载参数目录开始递归装载所有子目录及其jar文件
	 * 注意装载到类路径的都是目录而不是文件,而jar文件则被视为一种特殊的目录被装载到类路径,所以除jar文件以外的普通文件是不能被装载到类路径的
	 */
	public static void addFileToCurrentClassPath(File file,ClassLoader classLoader){
		if(file.isFile() && !file.getName().endsWith(".jar")) return;
		
		try {
			addClassPathMethod.invoke(classLoader, file.toURI().toURL());
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		if(file.isFile()) return;
		
		File[] fileArray = file.listFiles();
		for(File subFile:fileArray) addFileToCurrentClassPath(subFile,classLoader);
	}
	
	/**
	 * 获取独立文件的类装载器
	 * @param fileFullPath 文件全路径
	 * @return 文件类装载器
	 * @throws IOException IO异常
	 */
	public static ClassLoader getFileClassLoader(String fileFullPath) throws IOException{
		File file=new File(fileFullPath);
		if(!file.exists()){
			throw new FileNotFoundException(fileFullPath+" is not exists...");
		}
		if(file.isDirectory()){
			throw new IOException(fileFullPath+" is directory...");
		}
		return new URLClassLoader(new URL[]{file.toURI().toURL()});
	}
	
	/**
	 * 获取独立目录的类装载器
	 * @param path 路径目录
	 * @return 目录类装载器
	 * @throws IOException IO异常
	 */
	public static ClassLoader getPathClassLoader(String path) throws IOException{
		File fileDir=new File(path);
		if(!fileDir.exists()){
			throw new FileNotFoundException(path+" is not exists...");
		}
		if(fileDir.isFile()){
			throw new IOException(path+" is file...");
		}
		
		File[] jarFiles = fileDir.listFiles(new FilenameFilter() {  
			public boolean accept(File dir, String fileName) {  
				return fileName.endsWith(".jar");  
			}  
		});
		
		URL[] urls=new URL[jarFiles.length];
		for(int i=0;i<jarFiles.length;urls[i]=jarFiles[i].toURI().toURL(),i++);
		return new URLClassLoader(urls);
	}
	
	/**
	 * 判断指定的类是否存在于类路径中
	 * @param classFullName 类的全限定名
	 * @param type 类装载器参考类
	 * @return 是否存在该类
	 */
	public static boolean existClass(String classFullName,Class<?>... type){
		return null==loadType(classFullName,getCallerClassLoader(type))?false:true;
	}
	
	
	/**
	 * 判断指定的类是否存在于类路径中
	 * @param classFullName 类的全限定名
	 * @param classLoader 类装载器
	 * @return 是否存在该类
	 */
	public static boolean existClass(String classFullName,ClassLoader classLoader){
		return null==loadType(classFullName,classLoader)?false:true;
	}
	
	/**
	 * 装载类(未初始化)到方法区
	 * @param classFullName 类的全限定名
	 * @param type 类装载器参考类
	 * @return 类
	 */
	public static Class<?> loadType(String classFullName,Class<?>... type){
		try {
			return loadClass(classFullName,getCallerClassLoader(type));
		} catch (ClassNotFoundException e) {
			return null;
		}
	}
	
	/**
	 * 装载类(未初始化)到方法区
	 * @param classFullName 类的全限定名
	 * @param classLoader 类装载器
	 * @return 类
	 */
	public static Class<?> loadType(String classFullName,ClassLoader classLoader){
		try {
			return loadClass(classFullName,classLoader);
		} catch (ClassNotFoundException e) {
			return null;
		}
	}
	
	/**
	 * 装载类(未初始化)到方法区
	 * @param classFullName 类的全限定名
	 * @param type 类装载器参考类
	 * @return 类
	 * @throws ClassNotFoundException 找不到类的异常
	 */
	public static Class<?> loadClass(String classFullName,Class<?>... type) throws ClassNotFoundException{
		return loadClass(classFullName,getCallerClassLoader(type));
	}
	
	/**
	 * 装载类(未初始化)到方法区
	 * @param classFullName 类的全限定名
	 * @param classLoader 类装载器
	 * @return 类
	 * @throws ClassNotFoundException 找不到类的异常
	 */
	public static Class<?> loadClass(String classFullName,ClassLoader classLoader) throws ClassNotFoundException{
		if(null==classLoader) return null;
		return classLoader.loadClass(classFullName);
	}
	
	/**
	 * 实例化类的对象到堆区
	 * @param classFullName 类的全限定名
	 * @param type 类装载器参考类
	 * @return 对象类型
	 * @throws Exception 异常
	 */
	public static Object instanceClass(String classFullName,Class<?>... type) throws Exception{
		return instanceClass(classFullName,Object.class,getCallerClassLoader(type));
	}
	
	/**
	 * 实例化类的对象到堆区
	 * @param classFullName 类的全限定名
	 * @param returnType 返回类型
	 * @param type 类装载器参考类
	 * @return 泛化类型
	 * @throws Exception 异常
	 */
	public static <R> R instanceClass(String classFullName,Class<R> returnType,Class<?>... type) throws Exception{
		return instanceClass(classFullName,returnType,getCallerClassLoader(type));
	}
	
	/**
	 * 实例化类的对象到堆区
	 * @param classFullName 类的全限定名
	 * @param returnType 返回类型
	 * @param classLoader 类装载器
	 * @return 泛化类型
	 * @throws Exception 异常
	 */
	public static <R> R instanceClass(String classFullName,Class<R> returnType,ClassLoader classLoader) throws Exception{
		if(null==classLoader) return null;
		Class<?> type=Class.forName(classFullName, true, classLoader);
		return returnType.cast(type.newInstance());
	}
	
	/**
	 * 获取指定类路径中类的实例
	 * @param absoluteFile 绝对路径的文件名或目录名
	 * @param className 类的全限定名
	 * @return 对象类型
	 * @throws Exception 异常
	 */
	public static Object getInstance(String absoluteFile,String className) throws Exception{
		return getInstance(absoluteFile,className,Object.class);
	}
	
	/**
	 * 获取指定类路径中类的实例
	 * @param absoluteFile 绝对路径的文件名或目录名
	 * @param className 类的全限定名
	 * @param returnType 返回类型
	 * @return 泛化类型
	 * @throws Exception 异常
	 */
	public static <R> R getInstance(String absoluteFile,String className,Class<R> returnType) throws Exception{
		Class<?> cla=getClass(absoluteFile,className);
		if(null==cla) return null;
		return returnType.cast(cla.newInstance());
	}
	
	/**
	 * 获取指定类路径中的类
	 * @param file 绝对路径的文件名或目录名
	 * @param className 类的全限定名
	 * @return 类
	 * @throws Exception 异常
	 */
	public static Class<?> getClass(String absoluteFile,String className) throws Exception{
		if(null==absoluteFile||null==className||absoluteFile.trim().isEmpty()||className.trim().isEmpty()) return null;
		File file=new File(absoluteFile.trim());
		if(!file.exists()) return null;
		
		ClassLoader classLoader=null;
		if(file.isFile()){
			classLoader=getFileClassLoader(absoluteFile);
		}else{
			classLoader=getPathClassLoader(absoluteFile);
		}
		
		if(null==classLoader) return null;
		return classLoader.loadClass(className.trim());
	}
}
