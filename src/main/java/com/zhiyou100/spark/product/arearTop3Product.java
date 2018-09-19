package com.zhiyou100.spark.product;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.hive.ql.exec.spark.SparkUtilities;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.zhiyou100.dao.TaskDao;
import com.zhiyou100.factory.Factory;
import com.zhiyou100.pojo.Task;
import com.zhiyou100.utils.MockData;
import com.zhiyou100.utils.ParamUtils;

import scala.Tuple2;

public class arearTop3Product {

	public static void main(String[] args) {
		// 环境
		// 1.从mysql里面获取参数
		// 构建spark上下文
		SparkConf conf = new SparkConf().setAppName("UserSessionVisitAnalyze").setMaster("local");
		JavaSparkContext scContext = new JavaSparkContext(conf);
		
//		SparkContext scContext = new SparkContext(conf);
		// 测试环境用SQLContext，生产环境用hiveContext
		SQLContext sqlContext = new SQLContext(scContext);
//		HiveContext hiveContext = new  HiveContext(scContext);    hiveContext.table("")
		// 添加数据（生成模拟数据）
		MockData.mock(scContext, sqlContext);

		// 首先查询出要执行的任务，获取任务参数
		// 数据库参数 startDate endDate 用户输入的页面流
		// [{"startDate":["2015-01-01"],"endDate":["2018-12-12"],"pageStream":["1,2,3,4,56,7"]}]
		TaskDao tdDao = Factory.getTaskFactory();
		Task task = tdDao.findById(2L);
		// System.out.println(task);
		String param = task.getTask_param();// param： json字符串

		JSONArray parseArray = JSONArray.parseArray(param);
		JSONObject jsonObject = parseArray.getJSONObject(0);
		
		//从hive里面获取用户指定日期范围内的点击行为：城市id，点击的商品id
		
		//获取开始时间和结束时间
		String startDate = ParamUtils.getParam(jsonObject, "startDate");
		String endDate = ParamUtils.getParam(jsonObject, "endDate");
		
		String sql="select city_id ,click_product_id product_id from user_visit_action "
				+ "where click_product_id is not null and date between '"  +startDate+ "' and '"+endDate +"'" ;
/*		String sql="select city_id ,click_product_id product_id from user_visit_action "
				+ " where click_product_id is not null and date >= '"+startDate+"' and date <= '"+endDate+"'";
*/		
		DataFrame dateFrame = sqlContext.sql(sql);
		//dateFrame.show();
		
		JavaPairRDD<Long, Row> cityAction = dateFrame.toJavaRDD().mapToPair(new PairFunction<Row, Long, Row>() {

			public Tuple2<Long, Row> call(Row row) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2<Long, Row>(row.getLong(0),row);
			}
		});
		
		
		//从mysql里面获取城市信息
		
		String url="jdbc:mysql://localhost:3306/spark_bigdata24";
		String table="city_info";
		//驱动 账号 密码
		Properties properties=new Properties();
		properties.put("driver", "com.mysql.jdbc.Driver");
		properties.put("user", "root");
		properties.put("password", "123456");
		DataFrame cityDF = sqlContext.read().jdbc(url, table, properties);
//		cityDF.show();
		
		//转换为JavaRdd，并且映射为（cityId,row）
		JavaPairRDD<Long, Row> cityInfo = cityDF.toJavaRDD().mapToPair(new PairFunction<Row, Long, Row>() {

			public Tuple2<Long, Row> call(Row row) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2<Long, Row>((long)row.getInt(0), row);
			}
		});
		
		//cityId,productId    cityId,cityname area
		//进行聚合  生成商品基础信息的临时表
		JavaPairRDD<Long, Tuple2<Row, Row>> productInfo = cityAction.join(cityInfo);
		
		//cityId,cityName,area,productId
		JavaRDD<Row> productInfoRDD = productInfo.map(new Function<Tuple2<Long,Tuple2<Row,Row>>, Row>() {

			@SuppressWarnings("static-access")
			public Row call(Tuple2<Long, Tuple2<Row, Row>> row) throws Exception {
				Long cityId = row._1;
				Long productId = row._2._1.getLong(1);
				String cityName = row._2._2.getString(1);
				String area = row._2._2.getString(2);
				
				//给row赋值
				//参数类型（int...a）任意数量的int类型
				//   (Object....values)任意数量的object类型

				return new RowFactory().create(cityId,cityName,area,productId);
			}
		});
		
		//生成商品基础信息的临时表--需要dataframe
		//将RDD转换为dataFrame  javaRdd里面就是row ，dataFrame 里面是 schema+row
		
		//System.out.println(df);-->[city_id:bigint,product_id:bigint]
		
		//把schema信息拼接出来		
		List<StructField> structFields = new ArrayList<StructField>();
		structFields.add(DataTypes.createStructField("city_id", DataTypes.LongType, true));
		structFields.add(DataTypes.createStructField("city_name", DataTypes.StringType, true));
		structFields.add(DataTypes.createStructField("area", DataTypes.StringType, true));
		structFields.add(DataTypes.createStructField("product_id", DataTypes.LongType, true));
		
		
		//创建schema
		StructType schema = DataTypes.createStructType(structFields);
		
		
		//把javaRDD转换为dataframe
		
		DataFrame product_city = sqlContext.createDataFrame(productInfoRDD, schema);
		
		//product_city.show();
		/*
		 * +-------+---------+----+----------+
			|city_id|city_name|area|product_id|
			+-------+---------+----+----------+
			|      4|       三亚|  华南|        58|
			|      4|       三亚|  华南|        86|
			|      4|       三亚|  华南|        33|
			|      4|       三亚|  华南|        31|
		 */
		
		//javapairRdd->javaRdd->dataframe
		
		product_city.registerTempTable("tmp");
	   
//	    System.out.println( sqlContext.sql("select * from tmp").toJavaRDD().collect());
		
		
		
		
		//生成各个区域各商品的点击次数的临时表  以区域和商品id进行分组
		//目标数据：
		/*
		 * area 商品Id  点击次数  城市信息
		 * 华北      5        100  1：河南 
		 */
		String sql2="select "
				+ " area ,product_id,count(city_id) as clickCount  "
				+ "  from tmp "
				+ " group by area,product_id";
//		sqlContext.sql(sql2).show();
		/*
		 * |area|product_id|clickCount|
			+----+----------+----------+
			|  华北|        68|         1|
			|  东北|         6|         4|
			|  西北|        72|         6|
			|  华北|        69|         1|
			|  东北|         7|         4|
		 */
		
		//注册成临时表
		DataFrame area_click = sqlContext.sql(sql2);
		area_click.registerTempTable("tmp_area_click");
		
		//把各个区域各个商品的点击次数以及该商品的详细信息查找出来
		//sql select * from 商品点击次数  join 商品详情表
		//实际中：去hive里面进行查询商品详情表，注册成临时表
		
/*		String sql3="select * from tmp_area_click ac "
				+ " left join product_info pi on "
				+ " ac.product_id=pi.product_id ";*/
		
		//extend_info  包括商品的价格  商品的描述  商品的类别  商品的状态
		//利用json字符串进行存储
		
		//解析json字符串，在查询到的时候进行json的转换
		
		//使用自定义的UDF函数 - 把json字符串转换出来
		//注册udf			//函数名   函数   返回值类型
		sqlContext.udf().register("parseJson", new ParseJsonUDF(),DataTypes.StringType);
		
		/*
		 * if(条件，符合条件返回的值，不符合条件返回的值)
		 */
		String sql3="select * ,if(parseJson(extend_info,'product_status')=0,'自营', '第三方') 运营方 , "
				+ " case "
				+ "		when area='东北' then 'A级' "
				+ "     when area='西北' then 'B级' " 
				+ " 	else 'c级' "
				+ "end Level" 
				+ " from tmp_area_click ac "
				+ " left join product_info pi on "
				+ " ac.product_id=pi.product_id ";
		
		/*
		 * +----+----------+----------+----------+------------+--------------------+---+
			|area|product_id|clickCount|product_id|product_name|         extend_info|_c6|
			+----+----------+----------+----------+------------+--------------------+---+
			|  东北|        31|         1|        31|   product31|{"product_status"...|  1|
			|  华东|        31|         7|        31|   product31|{"product_status"...|  1|
		 */
		/*
		 * 0或者1  0是自营  1是第三方
		 * 也可以在定义一个UDF函数， jiexi(0/1) if else
		 * 也可以使用sparkSql里面内置的if函数
		 * 		String sql3="select * ,if(parseJson(extend_info,'product_status')=0,'自营', '第三方') "
				+ " from tmp_area_click ac "
				+ " left join product_info pi on "
				+ " ac.product_id=pi.product_id ";
		 */
		
		/*
		 * 商品的等级    A B C  中国  ，美国，河南，河北，经开区
		 * 如果是华北    A
		 * 		东北  B
		 * 		西北  C
		 *      否则   D
		 *利用case函数实现
		 *
		 *+----+----------+----------+----------+------------+--------------------+---+---+
		|area|product_id|clickCount|product_id|product_name|         extend_info|_c6|_c7|
		+----+----------+----------+----------+------------+--------------------+---+---+
		|  东北|        31|         5|        31|   product31|{"product_status"...|第三方| A级|
		|  华东|        31|         5|        31|   product31|{"product_status"...|第三方| c级|
		|  华中|        31|         9|        31|   product31|{"product_status"...|第三方| c级|
		 *
		 *
		 *		/*
		 * +----+----------+----------+----------+------------+--------------------+
			|area|product_id|clickCount|product_id|product_name|         extend_info|
			+----+----------+----------+----------+------------+--------------------+
			|  东北|        31|         1|        31|   product31|{"product_status"...|
			|  华东|        31|         8|        31|   product31|{"product_status"...|
		 */
		 
		
		DataFrame productInfoDf = sqlContext.sql(sql3);
		productInfoDf.show();
		
		//实现获取前三的功能
/*		String sql4="select *,row_number() over (partition by area ) "
				+ " from tmp_area_click ";
		DataFrame productInfoDf = sqlContext.sql(sql4);
		productInfoDf.show();*/

		
		
		
		
		
		
		
		
	}
}
