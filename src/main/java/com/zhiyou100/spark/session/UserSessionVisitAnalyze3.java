package com.zhiyou100.spark.session;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Optional;
import com.zhiyou100.constant.Constants;
import com.zhiyou100.dao.TaskDao;
import com.zhiyou100.factory.Factory;
import com.zhiyou100.pojo.SessionAggrAtat;
import com.zhiyou100.pojo.SessionDetail;
import com.zhiyou100.pojo.SessionRandomExtract;
import com.zhiyou100.pojo.Task;
import com.zhiyou100.utils.DateUtils;
import com.zhiyou100.utils.MockData;
import com.zhiyou100.utils.ParamUtils;
import com.zhiyou100.utils.StringUtils;
import com.zhiyou100.utils.ValidUtils;

import scala.Tuple2;
import scala.collection.parallel.ParIterable;

//时长和步长，过滤条件，获取随机session

/*
 * 项目是怎样运行的？
 * 	1.在j2ee平台创建任务，把任务的参数保存task表里面，填写的搜索词在param里面
 *  2.调用执行脚本执行sprk作业
 *  3.spark作业获取任务参数（从task表里面），开始执行任务
 */
public class UserSessionVisitAnalyze3 {
	private static JavaPairRDD<String, Row> reslut = null;

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
		// System.out.println(ParamUtils.getParam(jsonObject, "cities"));

		// 聚合操作
		JavaPairRDD<String, String> allAggrInfo = getAllAggrInfo(sqlContext);

		/*
		 * List<Tuple2<String, String>> list = allAggrInfo.collect(); for (int i
		 * = 0; i < list.size(); i++) { System.out.println(list.get(i)); }
		 */

		// 筛选数据（按条件）
		JavaPairRDD<String, String> filterAggrInfo = getfilter_data(allAggrInfo, jsonObject);

		/*
		 * List<Tuple2<String, String>> list = filter_data.collect(); for (int i
		 * = 0; i < list.size(); i++) { System.out.println(list.get(i)); }
		 */

		// 获取session的时长和步长，已及占比，并把数据写入数据库
		extractData(filterAggrInfo);

		// 筛选出的数据进行排序，取前10
		/*
		 * 1.聚合+筛选 2.获取时长步长 3.随机抽取 4.获取Topic10
		 */
		// 获取被点击过的所有的品类：返回的结果是品类Id
		/*
		 * 先过滤，在从符合条件的品类里面获取 过滤信息
		 * 
		 * 所需要的数据没有进行聚合，1.通过重构数据来进行数据的添加(推荐) 2.对数据进行join：筛选后的信息join session信息
		 * 尽量少使用RDD
		 */
		// <sessionId,(allAgrrInfo,row)>==><sessionId,row>
		JavaPairRDD<String, Row> filterSession = filterAggrInfo.join(reslut)
				.mapToPair(new PairFunction<Tuple2<String, Tuple2<String, Row>>, String, Row>() {

					public Tuple2<String, Row> call(Tuple2<String, Tuple2<String, Row>> tuple2) throws Exception {

						return new Tuple2<String, Row>(tuple2._1, tuple2._2._2);
					}
				});

		// 从过滤过的session里面获取所有的被点击（点击、支付、下单）过的商品品类
		JavaPairRDD<Long, Long> categoryIds = filterSession
				.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Row>, Long, Long>() {

					public Iterable<Tuple2<Long, Long>> call(Tuple2<String, Row> tuple2) throws Exception {
						// 获取row
						Row row = tuple2._2;
						List<Tuple2<Long, Long>> list = new ArrayList<Tuple2<Long, Long>>();

						// 把符合条件的商品Id添加到list中
						// 判断是否被点击
						Long clickCategoryId = row.getLong(6);
						if (clickCategoryId != null) {
							list.add(new Tuple2<Long, Long>(clickCategoryId, clickCategoryId));

						}

						// 判断是否被下单
						String orderCategoryIdss = row.getString(8);
						if (orderCategoryIdss != null) {
							// 里面可能有多个商品，需要将其还原成数组
							String[] orderCategoryIds = orderCategoryIdss.split(",");
							for (int i = 0; i < orderCategoryIds.length; i++) {
								list.add(new Tuple2<Long, Long>(Long.valueOf(orderCategoryIds[i]),
										Long.valueOf(orderCategoryIds[i])));
							}

						}

						// 判断是否支付
						String payCategoryIdss = row.getString(10);
						if (payCategoryIdss != null) {
							// 里面可能有多个商品，需要将其还原成数组
							String[] payCategoryIds = payCategoryIdss.split(",");
							for (int i = 0; i < payCategoryIds.length; i++) {
								list.add(new Tuple2<Long, Long>(Long.valueOf(payCategoryIds[i]),
										Long.valueOf(payCategoryIds[i])));
							}

						}

						return list;
					}
				});

		// 去重
		JavaPairRDD<Long, Long> categoryIDs = categoryIds.distinct();

		/*
		 * List<Tuple2<Long, Long>> list = categoryIds.collect(); for
		 * (Tuple2<Long, Long> tuple2 : list) { System.out.println(tuple2); }
		 */

		// 计算各个品类的点击次数，下单次数和支付次数

		// 统计点击次数
		JavaPairRDD<Long, Long> clickCategoryIdCount = getClickCategoryIdCount(filterSession);

		// 获取下单次数
		JavaPairRDD<Long, Long> orderCategoryIdCount = getCategoryIdCount(filterSession, 8);
		// System.out.println("-------"+orderCategoryIdCount.collect());

		// 获取购买次数
		JavaPairRDD<Long, Long> payCategoryIdCount = getCategoryIdCount(filterSession, 10);
		// System.out.println("======"+payCategoryIdCount.collect());

		// 点击过的商品 ，点击次数，订单次数，支付次数 join在一起
		JavaPairRDD<Long, Tuple2<Tuple2<Tuple2<Long, Optional<Long>>, Optional<Long>>, Optional<Long>>> categoryIdsClickOrderPay = categoryIDs
				.leftOuterJoin(clickCategoryIdCount).leftOuterJoin(orderCategoryIdCount)
				.leftOuterJoin(payCategoryIdCount);

		// (39,(((39,Optional.of(14)),Optional.of(10)),Optional.of(22)))
		// System.out.println(categoryIdsClickOrderPay.collect());

		// 进行格式转变==》（39,"categoryId=xxxx|ordercategoryId=xx|xx=xxx"）

		JavaPairRDD<Long, String> joinInfo = categoryIdsClickOrderPay.mapToPair(
				new PairFunction<Tuple2<Long, Tuple2<Tuple2<Tuple2<Long, Optional<Long>>, Optional<Long>>, Optional<Long>>>, Long, String>() {

					public Tuple2<Long, String> call(
							Tuple2<Long, Tuple2<Tuple2<Tuple2<Long, Optional<Long>>, Optional<Long>>, Optional<Long>>> tuple2)
							throws Exception {
						// (39,(((39,Optional.of(14)),Optional.of(10)),Optional.of(22)))
						Long categoryId = tuple2._1;
						Optional<Long> payCount1 = tuple2._2._2;// 购买次数
						Optional<Long> orderCount1 = tuple2._2._1._2;// 下单次数
						Optional<Long> clickCount1 = tuple2._2._1._1._2;// 点击次数

						// 获取支付次数
						long payCount = 0;
						if (payCount1.isPresent()) {
							payCount = payCount1.get();
						}
						// 获取下单次数
						long orderCount = 0;
						if (orderCount1.isPresent()) {
							orderCount = orderCount1.get();
						}
						// 获取点击次数
						long clickCount = 0;
						if (clickCount1.isPresent()) {
							clickCount = clickCount1.get();
						}

						// 二次排序，可以放在这里完成，将数据封装到对象中
						String joinInfo = Constants.FIELD_CATEGORY_ID + "=" + categoryId + "|"
								+ Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCount + "|"
								+ Constants.FIELD_ORDER_COUNT + "=" + orderCount + "|" + Constants.FIELD_PAY_COUNT + "="
								+ payCount;

						return new Tuple2<Long, String>(categoryId, joinInfo);
					}
				});

		// System.out.println(joinInfo.collect());

		// 二次排序
		JavaPairRDD<CategorySort, String> SortCount = joinInfo
				.mapToPair(new PairFunction<Tuple2<Long, String>, CategorySort, String>() {

					public Tuple2<CategorySort, String> call(Tuple2<Long, String> tuple2) throws Exception {
						String joininfo = tuple2._2;
						String clickCount = StringUtils.getFieldFromConcatString(joininfo, "\\|",
								Constants.FIELD_CLICK_CATEGORY_IDS);
						String orderCount = StringUtils.getFieldFromConcatString(joininfo, "\\|",
								Constants.FIELD_ORDER_COUNT);
						String payCount = StringUtils.getFieldFromConcatString(joininfo, "\\|",
								Constants.FIELD_PAY_COUNT);

						CategorySort categorySort = new CategorySort(Long.valueOf(clickCount), Long.valueOf(orderCount),
								Long.valueOf(payCount));

						return new Tuple2<CategorySort, String>(categorySort, joininfo);
					}
				});
		SortCount = SortCount.sortByKey(false);
		// (CategorySort [clickCount=25, orderCount=12,
		// payCount=11],categoryid=1|clickCategoryIds=25|orderCount=12|payCount=11),

		// 前10的品类
		List<Tuple2<CategorySort, String>> list = SortCount.take(10);

		// 把商品写入数据库
		for (Tuple2<CategorySort, String> tuple22 : list) {
			String info = tuple22._2;
			String clickCount = StringUtils.getFieldFromConcatString(info, "\\|", Constants.FIELD_CLICK_CATEGORY_IDS);
			String orderCount = StringUtils.getFieldFromConcatString(info, "\\|", Constants.FIELD_ORDER_COUNT);
			String payCount = StringUtils.getFieldFromConcatString(info, "\\|", Constants.FIELD_PAY_COUNT);
			// System.out.println(clickCount);

		}

		/*
		 * 求top10热门品类里面top10活跃session
		 */

		// list 里面包含：商品id，点击次数，支付次数，订单次数 （只需要其中的商品id）
		// <商品id,商品id>
		List<Tuple2<Long, Long>> top10CategoryIdList = new ArrayList<Tuple2<Long, Long>>();

		// 把top10热门品类给遍历出来，取里面有用的，放到新的集合里面
		for (Tuple2<CategorySort, String> tuple22 : list) {
			String info = tuple22._2;
			String categoryId = StringUtils.getFieldFromConcatString(info, "\\|", Constants.FIELD_CATEGORY_ID);

			top10CategoryIdList.add(new Tuple2<Long, Long>(Long.valueOf(categoryId), Long.valueOf(categoryId)));
		}

		// 转换为rdd
		JavaPairRDD<Long, Long> top10CategoryIdRdd = scContext.parallelizePairs(top10CategoryIdList);

		// 计算top10品类被各个session点击的次数
		// .1计算各session对各个品类的点击次数，从过滤后的session里面获取
		// <sessionId,row> => <sessionId,Itreable>
		JavaPairRDD<String, Iterable<Row>> sessionDetail = filterSession.groupByKey();

		JavaPairRDD<Long, String> categoryIdCount = sessionDetail
				.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<Row>>, Long, String>() {

					public Iterable<Tuple2<Long, String>> call(Tuple2<String, Iterable<Row>> tuple2) throws Exception {
						// 计算出每个session对每品类的点击次数

						String sessionId = tuple2._1;
						// 获取iterator<Row>
						Iterator<Row> it = tuple2._2.iterator();
						// (categoryId,count)
						Map<Long, Long> map = new HashMap<Long, Long>();
						while (it.hasNext()) {
							Row row = it.next();
							// 判断商品品类是否为null,如果不为空，在原有的基础上相加
							if (row.get(6) != null) {
								// 获取品类id
								long categoryId = row.getLong(6);
								// 获取原有的点击次数
								Long count = map.get(categoryId);
								if (count == null) {
									count = 0L;
								}
								count++;
								map.put(categoryId, count);

							}
						}

						// 求的是每一个sessionId，对每一个品类id的点击次数 =》从里面获取top10的热门品类的点击次数
						// 返回结果 <categoryId,sessionId,count>
						List<Tuple2<Long, String>> list = new ArrayList<Tuple2<Long, String>>();

						// sessionId,(categoryId,count)--在map中
						Set<Entry<Long, Long>> mapEntrySet = map.entrySet();
						for (Map.Entry<Long, Long> entry : mapEntrySet) {
							Long categoryID = entry.getKey();
							Long count = entry.getValue();

							// list categoryId,sessionId+count

							list.add(new Tuple2<Long, String>(categoryID, sessionId + "," + count));

						}

						return list;
					}
				});

		// top10热门品类的id ，多个品类的被各个session的点击次数
		// （categoryId,(categoryId,session+count)）
		JavaPairRDD<Long, String> top10CategorySessionCountRDD = top10CategoryIdRdd.join(categoryIdCount)
				.mapToPair(new PairFunction<Tuple2<Long, Tuple2<Long, String>>, Long, String>() {

					public Tuple2<Long, String> call(Tuple2<Long, Tuple2<Long, String>> tuple2) throws Exception {

						return new Tuple2<Long, String>(tuple2._1, tuple2._2._2);
					}

				});// (11,920aca9e6fb24883b50aee3dcd0dcad5,1),
					// (11,260be878d00b4edb831605a72f6b4114,1),
					// (11,84589dff4a4f426c82e6713ff7150795,1),
					// (11,1fd054020fab4938800788e1259b2c89,1),
					// (11,f60313a7bfda4822ba8741485852ca5f,1),
					// (11,358d7f63e7a14fbf970dbc15fdc2dc0a,1),
					// (11,5f2fe7c6284840ce87e54030fa1da340,1),
					// (11,fccc932c79f44b9084cd89458213af25,1),
					// (11,8e0e03670eb94d028be157a97de9c86d,1),
					// (11,8e2252d73301417facbe040f99ef4ae3,1),
					// (11,6249841566734528bac5b44c59df56d0,1)

		// System.out.println(top10CategorySessionCountRDD.collect());

		/*
		 * 分组取TopN算法，获取的是top10品类里面的前10个活跃session
		 */
		// 分组
		JavaPairRDD<Long, Iterable<String>> top10CategorySessionCount = top10CategorySessionCountRDD.groupByKey();
		// (84,[db226f10fb5b431687ddb1d7dd3d8d71,1,
		// 5dbd2c94b012475d9caec5d837f8bd1c,1]

		// System.out.println(top10CategorySessionCount.collect());

		// 只取sessionId
		top10CategorySessionCount
				.flatMapToPair(new PairFlatMapFunction<Tuple2<Long, Iterable<String>>, String, String>() {

					public Iterable<Tuple2<String, String>> call(Tuple2<Long, Iterable<String>> tuple2)
							throws Exception {
						Long categoryId = tuple2._1;

						// 获取点击该品类的sessionId及次数
						Iterator<String> iterator = tuple2._2.iterator();

						// 定义数组，接受存放的10个session
						String[] top10Session = new String[10];
						// 向数组内添加内容，并且排序
						while (iterator.hasNext()) {
							// db226f10fb5b431687ddb1d7dd3d8d71,1
							String sessionCount = iterator.next();

							// 遍历数组
							for (int i = 0; i < top10Session.length; i++) {
								// 如果当前位置，没有内容，把当前的session放入其中
								if (top10Session[i] == null) {
									top10Session[i] = sessionCount;
									break;
								} else {
									// 当前进来的数据
									long count = Long.valueOf(sessionCount.split(",")[1]);
									// 当前位置的数据
									Long _count = Long.valueOf(top10Session[i].split(",")[1]);
									if (count > _count) {
										// 当前位置的数据向后移，新加入的数据放在当前位置
										for (int j = 9; j > i; j--) {
											top10Session[j] = top10Session[j - 1];
										}
										top10Session[i] = sessionCount;
										break;
									}
								}
							}

						}

						// top10session
						for (int i = 0; i < top10Session.length; i++) {
							System.out.println(top10Session[i]);
						}
						// categoryID的前10个session 已经求出
						// 写入数据库
						List<Tuple2<String, String>> sessions = new ArrayList<Tuple2<String, String>>();

						for (int i = 0; i < top10Session.length; i++) {
							// categoryID
							String sessionID = top10Session[i].split(",")[0];
							Long count = Long.valueOf(top10Session[i].split(",")[1]);
							// 把数据添加到数据库中

							// 获取session详细信息，把session详细信息写入数据库
							// 根据sessionId区join session信息
							sessions.add(new Tuple2<String, String>(sessionID, sessionID));

						}
						return sessions;
					}
				});

	}

	// 聚合数据
	public static JavaPairRDD<String, String> getAllAggrInfo(SQLContext sqlContext) {
		// 开始执行任务

		String sql = "select * from user_visit_action";// 可以按时间进行筛选

		DataFrame dataframe = sqlContext.sql(sql);
		// dataframe.show();

		// 根据sessionId进行groupBy操作 （sessionId，Row）
		// 首先把datafram转换为javaRdd
		JavaRDD<Row> javaRDD = dataframe.toJavaRDD();

		// param:输入类型 输出的key 输出的value
		reslut = javaRDD.mapToPair(new PairFunction<Row, String, Row>() {

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

					public Tuple2<Long, String> call(Tuple2<String, Iterable<Row>> row) throws Exception {
						// TODO Auto-generated method stub
						// 获取搜索词，关键字。。。查询出来，聚合到一起
						String sessionId = row._1;
						Iterator<Row> iterator = row._2.iterator();// 迭代器

						StringBuffer serchKeyWords = new StringBuffer();
						StringBuffer clickCategoryIds = new StringBuffer();
						// 声明userId
						Long userId = null;

						// 定义开始时间和结束时间(需要进行计算，所以为date类型)
						Date startTime = null;
						Date endTime = null;

						long stepLength = 0;

						// 遍历迭代器
						while (iterator.hasNext()) {// 判断迭代器里是否有值
							Row row2 = iterator.next();// 取出迭代器里面的对象
							// 把搜索词和关键字取出来
							String searchKeyWord = row2.getString(5);
							String clickCategoryId = String.valueOf(row2.getLong(6));

							// 获取时间
							Date actionTime = DateUtils.parseTime(row2.getString(4));

							// 如果后面的时间比开始时间
							if (startTime == null) {
								startTime = actionTime;
							}
							if (endTime == null) {
								endTime = actionTime;
							}

							if (actionTime.before(endTime)) {
								startTime = actionTime;
							}
							if (actionTime.after(endTime)) {
								endTime = actionTime;
							}

							// 计算session的步长
							stepLength++;

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
						// 去逗号
						String searchkeyW = StringUtils.trimComma(serchKeyWords.toString());
						String clickCategoryI = StringUtils.trimComma(clickCategoryIds.toString());

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

						// 步长不用计算
						// 计算时长
						long visitLength = (endTime.getTime() - startTime.getTime()) / 1000;

						// 把startTime转换为字符串里类型
						String startTimestr = DateUtils.formatTime(startTime);
						String endTimestr = DateUtils.formatTime(endTime);

						String partAggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionId + "|"
								+ Constants.FIELD_SEARCH_KEYWORDS + "=" + searchkeyW + "|"
								+ Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryI + "|"
								+ Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|" + Constants.FIELD_STEP_LENGTH
								+ "=" + stepLength + "|" + Constants.FIELD_START_TIME + "=" + startTimestr + "|"
								+ Constants.FIELD_END_TIME + "=" + endTimestr;

						return new Tuple2<Long, String>(userId, partAggrInfo);
					}
				});
		// (46,sessionId=b529479223bb462cae410e51056560f7|
		// searchKeywords=日本料理,新辣道鱼火锅|clickCategoryIds=68,0,90)

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

		JavaPairRDD<Long, Row> user_pariRdd = userRDD.mapToPair(new PairFunction<Row, Long, Row>() {

			public Tuple2<Long, Row> call(Row row) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2<Long, Row>(row.getLong(0), row);
			}
		});// (68,(sessionId=200ceaa2873a42028ecea999e2e4e669|searchKeywords=国贸大厦,温泉,重庆辣子鸡|
			// clickCategoryIds=48,0,69,24,
			// [68,user68,name68,11,professional30,city69,male]))

		JavaPairRDD<Long, Tuple2<String, Row>> action_infoRDD = parAggtInfor.join(user_pariRdd);
		// action_infoRDD的数据类型 （Long,(String,row))）
		// 聚合之后，继续进行拼接字符串（sessionId,serarchKeyWords=x|....user_id=xx|...）
		@SuppressWarnings("serial")
		JavaPairRDD<String, String> allAggrInfo = action_infoRDD
				.mapToPair(new PairFunction<Tuple2<Long, Tuple2<String, Row>>, String, String>() {

					public Tuple2<String, String> call(Tuple2<Long, Tuple2<String, Row>> row) throws Exception {
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
								+ profession + "|" + Constants.FIELD_CITY + "=" + city + "|" + Constants.FIELD_SEX + "="
								+ sex;

						return new Tuple2<String, String>(sessionId, allAggrInfo);
					}

				});
		// (e38451b14e0c4b869bc3f92bd24211ac,
		// sessionId=e38451b14e0c4b869bc3f92bd24211ac|searchKeywords=|clickCategoryIds=0|
		// user_id=7|age=5|professional=professional80|city=city55|sex=male)

		return allAggrInfo;

	}

	// 过滤数据
	public static JavaPairRDD<String, String> getfilter_data(JavaPairRDD<String, String> data, JSONObject jsonObject) {
		// 筛选
		// 1.获取参数
		String startAge = ParamUtils.getParam(jsonObject, Constants.PARAM_START_AGE);
		String endAge = ParamUtils.getParam(jsonObject, Constants.PARAM_END_AGE);
		String zhiye = ParamUtils.getParam(jsonObject, Constants.PARAM_PROFESSIONALS);
		String cities = ParamUtils.getParam(jsonObject, Constants.PARAM_CITIES);
		String sex = ParamUtils.getParam(jsonObject, Constants.PARAM_SEX);
		String keyWords = ParamUtils.getParam(jsonObject, Constants.PARAM_KEYWORDS);
		String categoryIds = ParamUtils.getParam(jsonObject, Constants.PARAM_CATEGORY_IDS);

		// 2.需要把这些获取到的参数拼接成一个整体，并且还可以从你这个整体里面拿到想要的内容；
		// 把格式做成key=value的形式，多个用 | 隔开 city=null -> ""
		// 另外还要考虑里面某些字段可能是null

		// int a=(1>2)?3:4; 三目、三元运算符
		String params = (startAge != null ? Constants.PARAM_START_AGE + "=" + startAge + "|" : "")
				+ ((endAge != null) ? Constants.PARAM_END_AGE + "=" + endAge + "|" : "")
				+ (zhiye != null ? Constants.PARAM_PROFESSIONALS + "=" + zhiye + "|" : "")
				+ (cities != null ? Constants.PARAM_CITIES + "=" + cities + "|" : "")
				+ (sex != null ? Constants.PARAM_SEX + "=" + sex + "|" : "")
				+ (keyWords != null ? Constants.PARAM_KEYWORDS + "=" + keyWords + "|" : "")
				+ (categoryIds != null ? Constants.PARAM_CATEGORY_IDS + "=" + categoryIds + "|" : "");
		// 3.最后一个|需要去掉
		if (params.endsWith("\\|")) {
			params = params.substring(0, params.length() - 1);
		}

		final String _param = params;

		// 定义累加器
		final Accumulator<String> sessionAccumulator = new Accumulator<String>("", new SessionAggrStatAccumulator());

		// 4.根据筛选参数进行过滤 allAggrInfo
		JavaPairRDD<String, String> filter_age = data.filter(new Function<Tuple2<String, String>, Boolean>() {

			public Boolean call(Tuple2<String, String> tuple) throws Exception {
				// 1.获取到聚合参数
				String aggrInfo = tuple._2;

				// 按照性别进行过滤
				if (!ValidUtils.in(aggrInfo, Constants.FIELD_SEX, _param, Constants.PARAM_SEX)) {
					return false;
				}

				// 按照城市进行过滤
				/*
				 * if (!ValidUtils.equal(aggrInfo, Constants.FIELD_CITY, _param,
				 * Constants.PARAM_CITIES)) { return false; }
				 */
				/*
				 * if (!ValidUtils.equal(aggrInfo, Constants.FIELD_PROFESSIONAL,
				 * _param, Constants.PARAM_PROFESSIONALS)) { return false; }
				 */

				/**
				 * 统计时长和步长：
				 */

				// 获取时长和步长
				long visitLength = Long
						.valueOf(StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_VISIT_LENGTH));
				long stepLength = Long
						.valueOf(StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_STEP_LENGTH));

				if (visitLength >= 1 && visitLength <= 3) {
					sessionAccumulator.add(Constants.TIME_PERIOD_1s_3s);
				} else if (visitLength >= 4 && visitLength <= 6) {
					sessionAccumulator.add(Constants.TIME_PERIOD_4s_6s);
				} else if (visitLength >= 10 && visitLength <= 30) {
					sessionAccumulator.add(Constants.TIME_PERIOD_10s_30s);
				}

				if (stepLength >= 0 && stepLength < 30) {
					sessionAccumulator.add(Constants.STEP_PERIOD_10_30);
				} else if (stepLength >= 30 && stepLength <= 60) {
					sessionAccumulator.add(Constants.STEP_PERIOD_30_60);
				} else if (stepLength > 60) {
					sessionAccumulator.add(Constants.STEP_PERIOD_60);
				}

				// sesson总数量
				sessionAccumulator.add(Constants.SESSION_COUNT);

				return true;
			}
		});

		// transformation算子不会执行，需要action算子触发
		filter_age.collect();

		// 查看时长步长的个数
		System.out.println("============" + sessionAccumulator.value());

		/*
		 * String accumulator=sessionAccumulator.value(); String sessionCount =
		 * StringUtils.getFieldFromConcatString( accumulator, "\\|",
		 * Constants.SESSION_COUNT); String visti_0s_3s =
		 * StringUtils.getFieldFromConcatString( accumulator, "\\|",
		 * Constants.TIME_PERIOD_1s_3s); String visti_4s_6s =
		 * StringUtils.getFieldFromConcatString( accumulator, "\\|",
		 * Constants.TIME_PERIOD_4s_6s); String visti_10s_30s =
		 * StringUtils.getFieldFromConcatString( accumulator, "\\|",
		 * Constants.TIME_PERIOD_10s_30s); String step_0_30 =
		 * StringUtils.getFieldFromConcatString( accumulator, "\\|",
		 * Constants.STEP_PERIOD_10_30); String step_30_60 =
		 * StringUtils.getFieldFromConcatString( accumulator, "\\|",
		 * Constants.STEP_PERIOD_30_60); String step_61 =
		 * StringUtils.getFieldFromConcatString( accumulator, "\\|",
		 * Constants.STEP_PERIOD_60);
		 * 
		 * 
		 * 
		 * //将数据写入数据库中 SessionAggrAtat sessionAggrAtat=new SessionAggrAtat();
		 * sessionAggrAtat.setTask_id(1);
		 * sessionAggrAtat.setSession_count(Integer.valueOf(sessionCount));
		 * sessionAggrAtat.setVisti_0s_3s((Double.valueOf(visti_0s_3s))/(Double.
		 * valueOf(sessionCount)));
		 * sessionAggrAtat.setVisti_4s_6s(Double.valueOf(visti_4s_6s)/(Double.
		 * valueOf(sessionCount)));
		 * sessionAggrAtat.setVisti_10s_30s(Double.valueOf(visti_10s_30s)/(
		 * Double.valueOf(sessionCount)));
		 * sessionAggrAtat.setStep_0_30(Double.valueOf(step_0_30)/(Double.
		 * valueOf(sessionCount)));
		 * sessionAggrAtat.setStep_30_60(Double.valueOf(step_30_60)/(Double.
		 * valueOf(sessionCount)));
		 * sessionAggrAtat.setStep_61(Double.valueOf(step_61)/(Double.valueOf(
		 * sessionCount)));
		 * 
		 * Factory.getTaskFactory().insertSessionAggrAtat(sessionAggrAtat);
		 * 
		 */

		return filter_age;
	}

	// 随机抽取数据
	public static void extractData(JavaPairRDD<String, String> filterAggrInfo) {
		// 获取session的时长和步长，已及占比，并把数据写入数据库
		/*
		 * 获取时长 首先需要获取session的date 从而获取session的开始时间和结束时间 结束时间-开始时间=session的时长
		 * 
		 * 之前有一个聚合操作，但是聚合操作没有开始时间，也没有结束时间
		 * 
		 * 新聚合的RDD里面包含：开始时间，结束时间，用户信息，关键字，搜索词，点击品类及其他；
		 * 
		 * 可以重构之前的聚合功能，直接使用
		 * 
		 * 统计session的时长步长是在过滤功能执行之后再去统计的
		 * 
		 * 不重新定义RDD的话，直接重构聚合功能和过滤功能就可以了
		 * 
		 * 项目优化经验： 1.尽量少生成RDD 2.尽量少的对RDD进行算子计算；如果有可能，尽量在一个RDD里面实现多个需要做的功能。
		 * 3.尽可能少的对RDD进行shuffle计算，比如groupByKey，reduceByKey。。mapTopair
		 * 原因：使用shuffle会导致大量的磁盘读写，降低性能 容易造成数据倾斜；数据倾斜会导致性能降低
		 * 有shuffle算子和没有shuffle算子的性能差别是非常大的
		 * 
		 * 大数据项目性能第一 java项目-项目的安全性、可扩展性、可维护性
		 * 
		 * 
		 * 
		 * 
		 */

		// 4.计算出每天每小时的session数量 （yyyy-MM-dd_HH : sessionId）
		// 筛选过后数据（同一个sessionID,算一条数据） （user_visit_action）
		// 怎么判断sessionId是哪个时间段的：靠的是开始时间
		// 需要重构聚合功能，添加开始时间
		// 4.1 重构session聚合功能

		// 4.2 获取每天每小时的session数量 key是yyyy-MM-dd_HH,value是所有的相关信息

		JavaPairRDD<String, String> randomAggrData = filterAggrInfo
				.mapToPair(new PairFunction<Tuple2<String, String>, String, String>() {

					public Tuple2<String, String> call(Tuple2<String, String> tuple) throws Exception {

						String aggrInfo = tuple._2;
						// 获取sessionId
						String sessionId = tuple._1;
						// 获取开始时间
						String startTime = StringUtils.getFieldFromConcatString(aggrInfo, "\\|",
								Constants.FIELD_START_TIME);
						// 把开始时间转换成yyyy-MM-dd_HH的格式
						String starttime = DateUtils.getDateHour(startTime);

						return new Tuple2<String, String>(starttime, aggrInfo);
					}
				});// (2018-08-30_22,sessionId=486f96539965499fbe76a67c67d81ded|searchKeywords=温泉,火锅,蛋糕|
					// clickCategoryIds=35,0|visitLength=2887|stepLength=13|startTime=2018-08-30
					// 22:09:09|
					// user_id=21|age=33|professional=professional2|city=city14|sex=female)

		/*
		 * List<Tuple2<String, String>> list = RandomAggrData.collect(); for
		 * (int i = 0; i < list.size(); i++) { System.out.println(list.get(i));
		 * 
		 * }
		 */

		// 统计每天每小时的数量
		Map<String, Object> randomCountBy = randomAggrData.countByKey();
		/*
		 * {2018-08-30_21=23, 2018-08-30_09=23, 2018-08-30_15=28,
		 * 2018-08-30_19=24, 2018-08-30_14=14, 2018-08-30_02=24,
		 * 2018-08-30_05=23, 2018-08-30_10=15, 2018-08-30_11=20,
		 * 2018-08-30_18=32, 2018-08-30_22=22, 2018-08-30_01=14,
		 * 2018-08-30_06=22, 2018-08-30_00=20, 2018-08-30_12=15,
		 * 2018-08-30_17=26, 2018-08-30_08=14, 2018-08-30_07=29,
		 * 2018-08-30_16=21, 2018-08-30_03=16, 2018-08-30_13=28,
		 * 2018-08-30_20=19, 2018-08-30_04=23}
		 * 
		 */

		// 4.3 实现按时间比例随机抽取算法
		// 随机计算出每小时要抽取的session的索引，然后根据索引去获取内容
		/*
		 * 已知的是每天每小时的session数量；还需要计算出每天的session数量
		 * 现在的时间是：(yyyy-MM-dd_HH,count)->(yyyy-MM-dd,(HH,count))
		 * {2018-08-30={21=13,09=23...},2018-08-31={....}}--->应该实现的数据形式
		 * 
		 */

		// 改变数据格式为：(yyyy-MM-dd,(HH,count))
		Map<String, Map<String, Long>> dataHourCountMap = new HashMap<String, Map<String, Long>>();

		Set<Entry<String, Object>> entrySet = randomCountBy.entrySet();// 返回set集合

		for (Entry<String, Object> entry : entrySet) {
			// 获取时间 yyyy-MM-dd_HH
			String dataHour = entry.getKey();
			String[] split = dataHour.split("_");
			String data = split[0];
			String hour = split[1];

			// 取出count
			long count = (Long) entry.getValue();

			Map<String, Long> map = dataHourCountMap.get(data);
			if (map == null) {
				map = new HashMap<String, Long>();
			}

			map.put(hour, count);

			dataHourCountMap.put(data, map);
		}
		/*
		 * {2018-08-30={11=23, 22=27, 00=22, 01=21, 12=10, 02=26, 13=22, 14=16,
		 * 03=23, 15=19, 04=14, 05=18, 16=18, 06=20, 17=16, 18=28, 07=24, 19=28,
		 * 08=21, 09=19, 20=23, 21=14, 10=13}}----每天每小时的数据
		 */

		// dataHourCountMap.size();统计出一共有多少天

		System.out.println(dataHourCountMap);

		/*
		 * 从筛选出的session里面按时间随机抽取session
		 * 
		 * 1天有1000个session，要从中抽取100个session来进行分析 9:00-21:00 700个session
		 * 抽取70个session 21:00-9:00 300个session 抽取30个
		 * 
		 * 已知条件：session总数量 一共有多少天 每一天每一个小时产生了session
		 * 缺少一个需要抽取的session总数量---从task表里面进行获取 规定抽取100条数据
		 * 
		 * 
		 */

		// 获取每一天抽取的数量，然后在决定。每一天每个小时，应该抽取的数量
		int extractNumberEachDay = 100 / dataHourCountMap.size();

		// 按时间比例，随机抽取算法
		// date HH 数量
		// 应该算出 每一天每一个小时应该抽取出来的具体的session
		// 格式：<2018-08-30,<01,(3,4,7,5)>>-->3,4,7,5要抽取的sessionId的编号

		// 随机出要取的数据的索引

		final Map<String, Map<String, List<Integer>>> dateHourExtractMap = new HashMap<String, Map<String, List<Integer>>>();

		// 定义一个随机数对象
		Random random = new Random();
		// random.nextInt(10);//随机出一个0-10之间的数

		Set<Entry<String, Map<String, Long>>> dateHourCountMapSet = dataHourCountMap.entrySet();
		for (Entry<String, Map<String, Long>> dateHourCountEntry : dateHourCountMapSet) {
			String date = dateHourCountEntry.getKey();// 获取日期
			Map<String, Long> hourCountMap = dateHourCountEntry.getValue();

			// 要抽取的session数量/一天的session数量

			// 计算出每一天的session总数量 每个小时的数量相加
			Long sessionCount = 0L;
			Collection<Long> hourCountValues = hourCountMap.values();// 所有values的集合

			for (Long hourCountValue : hourCountValues) {
				sessionCount += hourCountValue;
			}
			// System.out.println(sessionCount);

			// 获取value，如果value==null，new一个value
			Map<String, List<Integer>> hourExtractMap = dateHourExtractMap.get(date);
			if (hourExtractMap == null) {
				hourExtractMap = new HashMap<String, List<Integer>>();

			}

			dateHourExtractMap.put(date, hourExtractMap);

			// 已知：一共要抽取的数量 100个 ，一共有多少天？dataHourCountMap.size()---》每一天要抽取多少个
			// 每天的session总量：sessionCount
			// 遍历每个小时产生session的数量

			Set<Entry<String, Long>> hourCount = hourCountMap.entrySet();
			for (Entry<String, Long> hourCountEntry : hourCount) {
				String hour = hourCountEntry.getKey();
				long count = hourCountEntry.getValue();

				// 每小时的session数量占总session的比例

				// 每小时抽取的session数量
				int hourExtractNumber = (int) (((double) extractNumberEachDay / (double) sessionCount)
						* (double) count);

				// ?每小时抽取的session数量，可不可能大于当前的小时的session数量

				if (hourExtractNumber > count) {
					hourExtractNumber = (int) (count);
				}

				// 每天每小时要抽取的session数据已经有了
				// 获取存放随机数的list
				List<Integer> extractIndexList = hourExtractMap.get(hour);
				if (extractIndexList == null) {
					extractIndexList = new ArrayList<Integer>();
				}

				hourExtractMap.put(hour, extractIndexList);

				// 生成随机数，把随机数放到list里面
				for (int i = 0; i < hourExtractNumber; i++) {
					int extractIndex = random.nextInt((int) count);

					// 随机5次，可能出现5个1
					while (extractIndexList.contains(extractIndex)) {
						extractIndex = random.nextInt((int) count);
					}

					extractIndexList.add(extractIndex);

				}

			}

		}

		System.out.println("==========" + dateHourExtractMap);

		// 6.遍历每一条每小时的session，然后根据随机索引进行抽取，抽取出来的数据存入数据库中
		JavaPairRDD<String, Iterable<String>> timeSessions = randomAggrData.groupByKey();
		/*
		 * List<Tuple2<String, Iterable<String>>> list = timeSessions.collect();
		 * for (int i = 0; i < list.size(); i++) {
		 * System.out.println(list.get(i)); }
		 */

		// mapTopair 一一映射
		/*
		 * 从一个里面获取很多个记录，使用flatmap算子，遍历所有的<date,AggrInfo> 然后遍历每天每小时的数据（多个session）
		 * 遍历的时候给每个session加上一个索引 0,1,2,3,4.。。
		 * 
		 * 如果发现某个session的索引在随机索引里面，
		 * 那么把session取出来，放到mysql数据库session_random_extract中
		 * 通过这个算子包sessionId返回回去，形成一个新的算子RDD(String-sessionId)
		 * 
		 * 使用者可以查看session的详情，所以还应把这些添加到session_detail中
		 * 在这里就可以根据抽取出来的sessionID去聚合信息
		 * 
		 * 
		 * 
		 */

		// 把符合条件的信息，添加到mysql数据库中，返回符合条件的sessionId
		JavaPairRDD<String, String> extractSessionIds = timeSessions
				.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<String>>, String, String>() {

					public Iterable<Tuple2<String, String>> call(Tuple2<String, Iterable<String>> t) throws Exception {
						// 获取时间：
						String datehour = t._1;// yyyy-MM-dd_HH
						String[] split = datehour.split("_");
						String date = split[0];
						String hour = split[1];

						// 获取内容
						Iterator<String> iterator = t._2.iterator();

						// 把随机数取出来
						List<Integer> extractIndexList = dateHourExtractMap.get(date).get(hour);

						int index = 0;

						List<Tuple2<String, String>> list = new ArrayList<Tuple2<String, String>>();

						while (iterator.hasNext()) {
							String aggrInfo = iterator.next();

							if (extractIndexList.contains(index)) {
								// 符合条件的数据，取出，放入数据库中
								String sessionId = StringUtils.getFieldFromConcatString(aggrInfo, "\\|",
										Constants.FIELD_SESSION_ID);
								String startTime = StringUtils.getFieldFromConcatString(aggrInfo, "\\|",
										Constants.FIELD_START_TIME);
								String endTime = StringUtils.getFieldFromConcatString(aggrInfo, "\\|",
										Constants.FIELD_END_TIME);
								String searchKeywords = StringUtils.getFieldFromConcatString(aggrInfo, "\\|",
										Constants.FIELD_SEARCH_KEYWORDS);

								SessionRandomExtract sessionRandomExtract = new SessionRandomExtract(1, sessionId,
										startTime, endTime, searchKeywords);
								Factory.getTaskFactory().insertSessionRandomExtract(sessionRandomExtract);
								// 把需要的信息获取到，放入数据库中
								list.add(new Tuple2<String, String>(sessionId, sessionId));

							}
							index++;
						}
						return list;
					}
				});

		// 聚合更详细的信息：sessionId，与user_visit_action进行聚合
		JavaPairRDD<String, Tuple2<String, Row>> session_detail = extractSessionIds.join(reslut);

		List<Tuple2<String, Tuple2<String, Row>>> collect = session_detail.collect();
		// System.out.println("===="+collect);
		/*
		 * for (Tuple2<String, Tuple2<String, Row>> tuple2 : collect) {
		 * System.out.println(tuple2); }
		 */

		// 把这个RDD里面的信息放到mysql里面
		session_detail.foreach(new VoidFunction<Tuple2<String, Tuple2<String, Row>>>() {

			public void call(Tuple2<String, Tuple2<String, Row>> t) throws Exception {
				Row row = t._2._2;
				int userId = (int) row.getLong(1);
				String sessionId = t._2._1;
				int pageId = (int) row.getLong(3);
				String actionTime = row.getString(4);
				String searchKeywords = row.getString(5);
				int clickCatoryId = (int) row.getLong(6);
				int clickProductId = (int) row.getLong(7);
				String orderCategoryIds = row.getString(8);
				String orderProductIds = row.getString(9);
				String payCategoryIds = row.getString(10);
				String payProductIds = row.getString(11);
				// System.out.println(row);
				// System.out.println(row.getString(0));
				SessionDetail sessionDetail = new SessionDetail(1, userId, sessionId, pageId, actionTime,
						searchKeywords, clickCatoryId, clickProductId, orderCategoryIds, orderProductIds,
						payCategoryIds, payProductIds);
				// Factory.getTaskFactory().insertSessionDetail(sessionDetail);

			}
		});

	}

	// 统计点击次数
	public static JavaPairRDD<Long, Long> getClickCategoryIdCount(JavaPairRDD<String, Row> filterSession) {
		// 统计次数 wordCount filter map reduce

		// filter 过滤点击为空的商品类别
		JavaPairRDD<String, Row> filterSession_filter = filterSession
				.filter(new Function<Tuple2<String, Row>, Boolean>() {

					public Boolean call(Tuple2<String, Row> v1) throws Exception {
						Row row = v1._2;
						// 这里不要用getLong(6)
						if (row.get(6) == null) {
							return false;
						} else {
							return true;
						}

					}
				});
		
		//测试随机key解决数据倾斜
		
		JavaPairRDD<String, Long> flilterSession_map = filterSession.mapToPair(
				new PairFunction<Tuple2<String,Row>, String, Long>() {

			public Tuple2<String, Long> call(Tuple2<String, Row> tuple2) 
					throws Exception {
				Row row = tuple2._2;
				long clickCategoryId = row.getLong(6);
				
				//添加随机key  (3_1,1)(3_1,1)(4_2,1)
				Random random=new Random();
				int prefix = random.nextInt(10);
				String rowkey=prefix+"_"+clickCategoryId;
				return new Tuple2<String, Long>(rowkey, 1L);
			}
		});
		//进行聚合
		JavaPairRDD<String, Long> randowkeyShuffle = flilterSession_map.reduceByKey(new Function2<Long, Long, Long>() {
			
			public Long call(Long v1, Long v2) throws Exception {
				// TODO Auto-generated method stub
				return v1 + v2;
			}
		});
		
		//取出前缀  
		JavaPairRDD<Long, Long> rmovePrefix = randowkeyShuffle.mapToPair(new PairFunction<Tuple2<String,Long>, Long, Long>() {

			public Tuple2<Long, Long> call(Tuple2<String, Long> tuple2) throws Exception {
				String key = tuple2._1.split("_")[1];
				return new Tuple2<Long, Long>(Long.valueOf(key), tuple2._2);
			}
		});
		
		//再次聚合
		JavaPairRDD<Long, Long> reduceByKey = rmovePrefix.reduceByKey(new Function2<Long, Long, Long>() {

			public Long call(Long v1, Long v2) throws Exception {
				// TODO Auto-generated method stub
				return v1+v2;
			}
		});
		
		
		

		// map (3,1)(4,1)...
/*		JavaPairRDD<Long, Long> flilterSession_map = filterSession_filter
				.mapToPair(new PairFunction<Tuple2<String, Row>, Long, Long>() {

					public Tuple2<Long, Long> call(Tuple2<String, Row> tuple2) throws Exception {
						Row row = tuple2._2;
						long clickCategoryId = row.getLong(6);

						return new Tuple2<Long, Long>(clickCategoryId, 1L);
					}
				});*/

		// reduce
		/*JavaPairRDD<Long, Long> filterSession_reduce = flilterSession_map
				.reduceByKey(new Function2<Long, Long, Long>() {

					public Long call(Long v1, Long v2) throws Exception {
						// TODO Auto-generated method stub
						return v1 + v2;
					}
				});*/


		return reduceByKey;
	}

	// 统计下单次数
	public static JavaPairRDD<Long, Long> getCategoryIdCount(JavaPairRDD<String, Row> filterSession, final int number) {
		// filter
		return filterSession.filter(new Function<Tuple2<String, Row>, Boolean>() {

			public Boolean call(Tuple2<String, Row> tuple2) throws Exception {
				Row row = tuple2._2;
				return row.getString(number) != null ? true : false;

			}
		}).flatMapToPair(new PairFlatMapFunction<Tuple2<String, Row>, Long, Long>() {

			public Iterable<Tuple2<Long, Long>> call(Tuple2<String, Row> tuple2) throws Exception {
				Row row = tuple2._2;
				String orderCategoryId = row.getString(number);
				String[] splits = orderCategoryId.split(",");

				List<Tuple2<Long, Long>> list = new ArrayList<Tuple2<Long, Long>>();
				for (String split : splits) {
					list.add(new Tuple2<Long, Long>(Long.valueOf(split), 1L));
				}

				return list;
			}
		}).reduceByKey(new Function2<Long, Long, Long>() {

			public Long call(Long v1, Long v2) throws Exception {
				// TODO Auto-generated method stub
				return v1 + v2;
			}
		});

	}

}
