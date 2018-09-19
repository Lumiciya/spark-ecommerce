package com.zhiyou100.utils;

import java.math.BigDecimal;

/**
 * 数字格工具类
 * @author Administrator
 *
 */
public class NumberUtils {

	/**
	 * 格式化小数
	 * @param str 字符串
	 * @param scale 四舍五入的位数
	 * @return 格式化小数
	 */
	public static double formatDouble(double num, int scale) {
		BigDecimal bd = new BigDecimal(num);  
		return bd.setScale(scale, BigDecimal.ROUND_HALF_UP).doubleValue();
	}
	
	public static void main(String[] args) {
		double d=1.2125;
		BigDecimal bd = new BigDecimal(d);
		System.out.println(bd);
		double doubleValue = bd.setScale(3,BigDecimal.ROUND_HALF_UP).doubleValue();
		System.out.println(doubleValue);
	}
	
}
