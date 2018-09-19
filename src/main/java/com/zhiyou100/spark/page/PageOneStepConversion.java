package com.zhiyou100.spark.page;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.zhiyou100.dao.TaskDao;
import com.zhiyou100.factory.Factory;
import com.zhiyou100.pojo.Task;

import com.zhiyou100.utils.DateUtils;
import com.zhiyou100.utils.MockData;
import com.zhiyou100.utils.NumberUtils;
import com.zhiyou100.utils.ParamUtils;


import scala.Tuple2;

public class PageOneStepConversion {

	public static void main(String[] args) {
		// 环境
		// 1.从mysql里面获取参数
		// 构建spark上下文
		SparkConf conf = new SparkConf().setAppName("UserSessionVisitAnalyze").setMaster("local");
		JavaSparkContext scContext = new JavaSparkContext(conf);
		// 测试环境用SQLContext，生产环境用hiveContext
		SQLContext sqlContext = new SQLContext(scContext);

		// 添加数据（生成模拟数据）
		MockData.mock(scContext, sqlContext);

		// 首先查询出要执行的任务，获取任务参数
		// 数据库参数 startDate endDate 用户输入的页面流
		//[{"startDate":["2015-01-01"],"endDate":["2018-12-12"],"pageStream":["1,2,3,4,56,7"]}]
		TaskDao tdDao = Factory.getTaskFactory();
		Task task = tdDao.findById(2L);
//		System.out.println(task);
		String param = task.getTask_param();// param： json字符串

		JSONArray parseArray = JSONArray.parseArray(param);
		JSONObject jsonObject = parseArray.getJSONObject(0);
		
		//获取user_visit_action信息并生成RDD
		
		//2.获取开始时间和结束时间
		String startDate=ParamUtils.getParam(jsonObject, "startDate");
		String endDate=ParamUtils.getParam(jsonObject, "endDate");
		
		String sql="select * from user_visit_action where date between '"  +startDate+ "' and '"+endDate +"'" ;
		
		DataFrame dataFrame = sqlContext.sql(sql);
		
		JavaRDD<Row> actionRDD = dataFrame.toJavaRDD();
		
		
		//3.将其映射成（sessionId，访问行为）的RDD
		//session粒度的
		//A 1,2,3 B 1,3,5(不可以从A用户的1到B用户)
		
		//4.session粒度的按照sessionId进行聚合
		JavaPairRDD<String, Iterable<Row>> sessionAction = actionRDD.mapToPair(new PairFunction<Row, String, Row>() {

			public Tuple2<String, Row> call(Row row) throws Exception {
				
				return new Tuple2<String, Row>(row.getString(2),row);
			}
		}).groupByKey();
		
		//5.核心步骤----计算每个session的单跳页面切片的生成，以及页面流的匹配
		/*
		 * 用户输入的是：3,5,7,9
		 * 单跳页面切片：3_5,5_7,7_9
		 * 页面流：3,5,7,9
		 */
		
		//5.1获取页面流
		String pageStream = ParamUtils.getParam(jsonObject, "pageStream");
		//将数据从一个节点发送到其他节点上面
		final Broadcast<String> 
				pageStreamBroadcast = scContext.broadcast(pageStream);
		
		JavaPairRDD<String, Integer> pageSplit = sessionAction.flatMapToPair(new PairFlatMapFunction<Tuple2<String,Iterable<Row>>, String, Integer>() {

			public Iterable<Tuple2<String, Integer>> call(Tuple2<String, Iterable<Row>> tuple2) throws Exception {
				//1.获取rows
				Iterator<Row> iterator = tuple2._2.iterator();
				
				//2.获取用户输入的页面流 -要做匹配
				String pages = pageStreamBroadcast.value();
				
				//3.切割字符串
				String[] page = pages.split(",");
				
				/*
				 * session数据默认情况下，不一定是按照时间顺序排好的
				 * 但实际中用户的行为肯定是按照时间排序的，先点击谁，再点击谁
				 * 所以需要按照时间进行排序
				 */
				
				List<Row> list=new ArrayList<Row>();
				while(iterator.hasNext()){
					list.add(iterator.next());
				}
				
				//使用list进行排序（collection和collections）
				Collections.sort(list,new Comparator<Row>() {

					public int compare(Row o1, Row o2) {
						// TODO Auto-generated method stub
						String date1 = o1.getString(4);
						String date2 = o2.getString(4);
						Date d1 = DateUtils.parseTime(date1);
						Date d2 = DateUtils.parseTime(date2);
						return (int) (d1.getTime()-d2.getTime()) ;
					}
				});
				

				
				//获取到了页面输入流 ,sessionId里面的数据并且按照时间顺序排列好了
				
				//页面切片的生成  List<row - pageId 3,5,7,9>
				
				Long lastpageId=null;
				
				List<Tuple2<String, Integer>> list1=new ArrayList<Tuple2<String,Integer>>();
				for (Row row : list) {
					//获得pageId
					long pageId = row.getLong(3);
					
					if(lastpageId==null){
						lastpageId=pageId;
						continue;//结束当前循环，进入下次循环
					}
					
					//生成页面切面（3_5,5_7,7_9）
					
					String pageSplit=lastpageId+"_"+pageId;//3_5
															
					//匹配输入流	判断当前的页面切片，是否在页面输入流里面
					for(int i=1;i<page.length;i++){
						//1,2,3,4,5,6,7 => 1_2,2_3,3_4,,,,,,
						
						String inputSplit=page[i-1]+"_"+page[i];
						
						if(inputSplit.equals(pageSplit)){
							//匹配上了
							
							list1.add(new Tuple2<String, Integer>(pageSplit, 1));
							break;
						}						
					}					
					lastpageId=pageId;					
				}							
				return list1;
			}
		});//(5_6,1), (6_7,1), (6_7,1), (4_5,1)
		
		Map<String, Object> pageSplitCountByKey = pageSplit.countByKey();
		String[] split = pageStream.split(",");

		//{3_4=103, 2_3=94, 4_5=117, 6_7=90, 1_2=109, 5_6=102}
		
//		System.out.println(pageSplitCountByKey);
		
		
/*		//list的排序方法
		List<CategorySort> list2= new ArrayList<CategorySort>();
		list2.add(new CategorySort(100L, 70L, 80L));
		list2.add(new CategorySort(34L, 56L, 12L));
		list2.add(new CategorySort(67L, 34L, 80L));
//		Collections.sort(list2);
		//直接自定义排序规则
		Collections.sort(list2,new Comparator<CategorySort>() {

			public int compare(CategorySort o1, CategorySort o2) {
				return (int) (o1.getPayCount()-o2.getPayCount());
			}
		});
		
		
		
		
		for (CategorySort object : list2) {
			System.out.println(object);
		}*/
		
		
		//计算出页面流中初始页面的pv
		
		//首先要获取你的初始界面
		final Long startPageId =Long.valueOf(split[0]) ;
		
		//sessionAction  聚合的session粒度的数据（sessionId,Row）
		long pvCount = sessionAction.flatMap(new FlatMapFunction<Tuple2<String,Iterable<Row>>, Long>() {

			public Iterable<Long> call(Tuple2<String, Iterable<Row>> tuple2) throws Exception {
				Iterator<Row> iterator = tuple2._2.iterator();
				
				List<Long> list= new ArrayList<Long>();
				
				while (iterator.hasNext()) {
					Row row =iterator.next();
					
					//获取到pageId
					long pageId = row.getLong(3);
					
					if(pageId==startPageId){
						list.add(pageId);
					}										
				}				
				return list;
			}
		}).count();
		
//		System.out.println(pvCount);
		
		//计算页面切片转换率
		/*
		 * 1.匹配的页面切面（1_2=123,2_3=232）
		 * 2.初始页面的pv
		 * 3.如何计算切面的转换率：
		 * startpv lastpv  lastPv/startpv
		 * 
		 * 1的pv : 初始页面的pv
		 * 2的pv:1_2的数量
		 * start/1_2
		 */
		
		
		//获取页面的输入流  split
		Long lastPv=0L;
		
		//定义一个map，来接受转换率
		Map<String, Double> map =new HashMap<String, Double>();
		
		
		
		for(int i=1;i<split.length;i++){
			//获得页面切片
			String targePage=split[i-1]+"_"+split[i];
			
			//获取页面切片对应的pv
			Long targetPageSplitpv = (Long)(pageSplitCountByKey.get(targePage));
			
			//定义一个变量来接受转换率
			
			double converRate=0.00000000;
			if(i==1){
				converRate=(double)targetPageSplitpv/(double)pvCount;
			}else{
				converRate=(double)targetPageSplitpv/(double)lastPv;
			}
			
			lastPv=targetPageSplitpv;
			
			double formatDouble = NumberUtils.formatDouble(converRate, 3);
			
			
			//此时结果已经出来了  2_3=0.8
			map.put(targePage, formatDouble);
		}
		
	
		System.out.println(map);
		
		//写入数据库
		/*
		 * 主键id ,taskId,页面切面，转换率
		 */
		
		/*
		 * 计算
		 * 
		 */
		
		
	}

}
