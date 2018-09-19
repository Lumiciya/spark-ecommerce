package com.zhiyou100.spark.session;

import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.zhiyou100.constant.Constants;
import com.zhiyou100.dao.TaskDao;
import com.zhiyou100.factory.Factory;
import com.zhiyou100.pojo.Task;
import com.zhiyou100.utils.MockData;
import com.zhiyou100.utils.ParamUtils;
import com.zhiyou100.utils.StringUtils;

import scala.Tuple2;

/*
 * 项目是怎样运行的？
 * 	1.在j2ee平台创建任务，把任务的参数保存task表里面，填写的搜索词在param里面
 *  2.调用执行脚本执行sprk作业
 *  3.spark作业获取任务参数（从task表里面），开始执行任务
 */
public class UserSessionVisitAnalyze {
	public static void main(String[] args) {
		// 构建spark上下文
		SparkConf conf = new SparkConf().setAppName("UserSessionVisitAnalyze").setMaster("local");
		JavaSparkContext scContext = new JavaSparkContext(conf);
		// 测试环境用SQLContext，生产环境用hiveContext
		SQLContext sqlContext = new SQLContext(scContext);

		// 添加数据（生成模拟数据）
		MockData.mock(scContext, sqlContext);

		// 首先查询出要执行的任务，获取任务参数
		TaskDao tdDao = Factory.getTaskFactory();
		Task task = tdDao.findById(1L);
		String param = task.getTask_param();// param： json字符串

		JSONArray parseArray = JSONArray.parseArray(param);
		JSONObject jsonObject = parseArray.getJSONObject(0);
		System.out.println(ParamUtils.getParam(jsonObject, "sex"));

		// 聚合操作
		JavaPairRDD<String, String> allAggrInfo = getAllAggrInfo(sqlContext);

		List<Tuple2<String, String>> list = allAggrInfo.collect();
		for (int i = 0; i < list.size(); i++) {
			System.out.println(list.get(i));
		}

		// 筛选数据（按条件）
		
		
		
		
		
		
		

	}

	public static JavaPairRDD<String, String> getAllAggrInfo(SQLContext sqlContext) {
		// 开始执行任务

		String sql = "select * from user_visit_action";// 可以按时间进行筛选

		DataFrame dataframe = sqlContext.sql(sql);
		// dataframe.show();

		// 根据sessionId进行groupBy操作 （sessionId，Row）
		// 首先把datafram转换为javaRdd
		JavaRDD<Row> javaRDD = dataframe.toJavaRDD();

		// param1:输入类型  输出的key 输出的value
		JavaPairRDD<String, Row> reslut = javaRDD.mapToPair(new PairFunction<Row, String, Row>() {

			public Tuple2<String, Row> call(Row row) throws Exception {
				// TODO Auto-generated method stub
				// 获取session id
				String sessionId = row.getString(2);

				return new Tuple2<String, Row>(sessionId, row);
			}
		});

		JavaPairRDD<String, Iterable<Row>> visit_key = reslut.groupByKey();

		// 对每一个sessionId分组进行聚合，将session中的所有的搜索词，和点击品类进行聚合
		@SuppressWarnings("serial")
		JavaPairRDD<Long, String> parAggtInfor = visit_key
				.mapToPair(new PairFunction<Tuple2<String, Iterable<Row>>, Long, String>() {

					public Tuple2<Long, String> call(Tuple2<String, Iterable<Row>> row) 
							throws Exception {
						// TODO Auto-generated method stub
						// 获取搜索词，关键字。。。查询出来，聚合到一起
						String sessionId = row._1;
						Iterator<Row> iterator = row._2.iterator();// 迭代器

						StringBuffer serchKeyWords = new StringBuffer();
						StringBuffer clickCategoryIds = new StringBuffer();
						// 声明userId
						Long userId = null;

						// 遍历迭代器
						while (iterator.hasNext()) {// 判断迭代器里是否有值
							Row row2 = iterator.next();// 取出迭代器里面的对象
							// 把搜索词和关键字取出来
							String searchKeyWord = row2.getString(5);
							String clickCategoryId = String.valueOf(row2.getLong(6));

							// 相同sessionId的session，他们的userId肯定是同一个
							// userId相同的session，他们的sessionId不同

							if (userId == null) {
								userId = row2.getLong(1);
							}

							/*
							 * 1.要把他们拼接起来，首先不是null 其次还要满足之前的字符串中没有该搜索词或者点击品类
							 */
							if (StringUtils.isNotEmpty(searchKeyWord)) {
								if (!serchKeyWords.toString().contains(searchKeyWord)) {
									serchKeyWords.append(searchKeyWord + ",");
								}
							}
							if (StringUtils.isNotEmpty(clickCategoryId)) {
								if (!clickCategoryIds.toString().contains(clickCategoryId)) {
									clickCategoryIds.append(clickCategoryId + ",");
								}
							}
						}
						System.out.println(serchKeyWords.toString());
						// 去逗号
						String searchkeyW = StringUtils.trimComma(serchKeyWords.toString());
						String clickCategoryI = StringUtils.trimComma(clickCategoryIds.toString());
						System.out.println(searchkeyW);
						/**
						 * 如果说返回的是：（sessionId，String）,之后还要和user_info合并，user_info里面么有sessionId
						 * 能不能返回（userId,String）
						 * 如果返回的是（userId,String）,还是session粒度的了，key不是sessionId了
						 * 但是要与user_info执行join操作，必须返回（userId,String）
						 * 
						 * 能不能先join,（userId,user_info+user_visit_action）
						 * 但是我返回的数据是（sessionId,user_info+user_visit_action）
						 */

						// 聚合数据 sessinId,关键字+搜索词
						/*
						 * 定义一个聚合的格式 key=value|key=value|key=value
						 */
						String partAggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionId + "|"
								+ Constants.FIELD_SEARCH_KEYWORDS + "=" + searchkeyW + "|"
								+ Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryI;

						return new Tuple2<Long, String>(userId, partAggrInfo);
					}
				});
				// (46,sessionId=b529479223bb462cae410e51056560f7|
				//searchKeywords=日本料理,新辣道鱼火锅|clickCategoryIds=68,0,90)

		// 查看
		/*
		 * List<Tuple2<Long, String>> list = data.collect(); for (int i = 0; i <
		 * list.size(); i++) { System.out.println(list.get(i)); }
		 */

		// 与user_info进行聚合
		// 查询出用户信息
		DataFrame user_dataFrame = sqlContext.sql("select * from user_info");

		// 把user转换为（user_id,row）
		JavaRDD<Row> userRDD = user_dataFrame.javaRDD();

		JavaPairRDD<Long, Row> user_pariRdd = userRDD.mapToPair(
				new PairFunction<Row, Long, Row>() {

			public Tuple2<Long, Row> call(Row row) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2<Long, Row>(row.getLong(0), row);
			}
		});// (68,(sessionId=200ceaa2873a42028ecea999e2e4e669|searchKeywords=国贸大厦,温泉,重庆辣子鸡|
			//clickCategoryIds=48,0,69,24,
			// [68,user68,name68,11,professional30,city69,male]))

		JavaPairRDD<Long, Tuple2<String, Row>> action_infoRDD = parAggtInfor.join(user_pariRdd);
		// action_infoRDD的数据类型 （Long,(String,row))）
		// 聚合之后，继续进行拼接字符串（sessionId,serarchKeyWords=x|....user_id=xx|...）
		@SuppressWarnings("serial")
		JavaPairRDD<String, String> allAggrInfo = action_infoRDD
				.mapToPair(new PairFunction<Tuple2<Long, Tuple2<String, Row>>, String, String>() {

					public Tuple2<String, String> call(Tuple2<Long, Tuple2<String, Row>> row) 
							throws Exception {
						// 获取原先的聚合信息：serachKeyWords clickCategoryIds
						String partAgrInfo = row._2._1;
						// 获取user信息
						Row user = row._2._2;

						// 获取sessionId
						String sessionId = StringUtils.getFieldFromConcatString(partAgrInfo, "\\|",
								Constants.FIELD_SESSION_ID);
						// (sessionId,partAggrInfo+user的信息 userId=1|...)
						long user_id = user.getLong(0);
						int age = user.getInt(3);
						String profession = user.getString(4);
						String city = user.getString(5);
						String sex = user.getString(6);

						String allAggrInfo = partAgrInfo + "|" + Constants.FIELD_UserID + "=" + user_id + "|"
								+ Constants.FIELD_AGE + "=" + age + "|" + Constants.FIELD_PROFESSIONAL + "="
								+ profession + "|" + Constants.FIELD_CITY + "=" + city + "|" 
								+ Constants.FIELD_SEX + "=" + sex;

						return new Tuple2<String, String>(sessionId, allAggrInfo);
					}

				});
					// (e38451b14e0c4b869bc3f92bd24211ac,
					// sessionId=e38451b14e0c4b869bc3f92bd24211ac|searchKeywords=|clickCategoryIds=0|
					// user_id=7|age=5|professional=professional80|city=city55|sex=male)

		return allAggrInfo;

	}

}
