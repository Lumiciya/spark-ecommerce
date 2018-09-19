package com.zhiyou100.test;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

public class Test {
	
	public static void main(String[] args) {
		//匿名函数
		new Student() {
			
			public int getAge(int age) {
				// TODO Auto-generated method stub
				return 1;
			}
		};
		
		//累加器
		List<String> list = Arrays.asList("s","c","v","s");
		SparkConf conf = new SparkConf().setAppName("test").setMaster("local");
		JavaSparkContext scContext = new JavaSparkContext(conf);
		JavaRDD<String> rdd = scContext.parallelize(list);
		
		final Accumulator<Long> accumulator = new Accumulator(0L,new LongAccumulator() );
		
		final Accumulator a=scContext.accumulator(0);
		
		rdd.foreach(new VoidFunction<String>() {
			
			public void call(String t) throws Exception {
				
				if(t.equals("s")){
//					a.add(1);
					accumulator.add(1L);
				}
				System.out.println(t);
				
			}
		});
		System.out.println(accumulator.value());
		
		
		
		
		
	}

}
