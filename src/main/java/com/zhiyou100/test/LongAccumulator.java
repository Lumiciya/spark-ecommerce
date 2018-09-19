package com.zhiyou100.test;

import org.apache.spark.AccumulatorParam;

public class LongAccumulator implements AccumulatorParam<Long>{

	/**
	 * 3.执行完。。之后，最后执行，把计算结果+zero的值
	 */
	public Long addInPlace(Long v1, Long v2) {
		// TODO Auto-generated method stub
		return v1+v2;
	}

	/*
	 * 1.初始化方法
	 * 	返回的是累加器的初始值
	 */
	public Long zero(Long arg0) {
		// TODO Auto-generated method stub
		return 0L;
	}

	/*
	 *2.执行addAccumulator方法
	 *			add(1)
	 *		第一次执行  (zero,1)	--1
	 *		第二次执行   (1,1)	--2
	 */
	public Long addAccumulator(Long v1, Long v2) {
		// TODO Auto-generated method stub
		return v1+v2;
	}
	

}
