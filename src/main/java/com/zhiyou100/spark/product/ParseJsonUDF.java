package com.zhiyou100.spark.product;

import org.apache.spark.sql.api.java.UDF2;

import com.alibaba.fastjson.JSONObject;

/*
 * UDF2 传进来2个参数
 */
//T1 T2 是传进来的  R 是返回值
public class ParseJsonUDF implements UDF2<String, String, String>{


	public String call(String s1, String s2) throws Exception {
		
		JSONObject parseObject = JSONObject.parseObject(s1);
		Object object = parseObject.get(s2);
		return object+"";
	}
	
	
}
