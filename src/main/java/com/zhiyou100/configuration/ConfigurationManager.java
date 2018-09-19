package com.zhiyou100.configuration;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ConfigurationManager {
	//private:私有化，防止外部的类对他进行访问，获取外部更新里面的某个属性，可能会导致程序崩溃
	private static Properties properties = new Properties();

	//引入db.properties 只需要第一次执行的时候引入一次即可
	//需要使用静态代码块--会在类加载的时候，进行加载，只加载一次
	static{//反射获取类加载器，获取资源
		InputStream  in=  ConfigurationManager.class.getClassLoader().getResourceAsStream("db.properties");
		try {
			properties.load(in);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}//抛异常
		}
	
/*	public static void main(String[] args) {
		System.out.println(properties.getProperty("test")) ;
	}*/
	//暴露出一个方法，让外部的类使用
	public static String getPropertity(String key){
		return properties.getProperty(key);
	}


}
