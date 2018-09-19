package com.zhiyou100.dao.impl;

import java.sql.ResultSet;
import java.sql.SQLException;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.zhiyou100.dao.TaskDao;
import com.zhiyou100.factory.Factory;
import com.zhiyou100.pojo.SessionAggrAtat;
import com.zhiyou100.pojo.SessionDetail;
import com.zhiyou100.pojo.SessionRandomExtract;
import com.zhiyou100.pojo.Task;
import com.zhiyou100.utils.DBUtil;


public class TaskDaoImpl implements TaskDao {

	public Task findById(Long id) {
		String sql = "select * from task where task_id=?";
		Object[] obj = {id};		
		ResultSet result = DBUtil.getInstance().select(sql, obj);
		try {
			if(result.next()){
				return new Task(result.getLong(1), result.getString(2),result.getString(3), result.getString(4), result.getString(5), result.getString(6), result.getString(7), result.getString(8));
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
				
		return null;
	}

	public void insertSessionAggrAtat(SessionAggrAtat sessionAggrAtat) {
		String sql="insert into session_aggr_stat("
				+ "task_id,session_count,1s_3s,4s_6s,10s_30s,10_30,30_60,60_)"
				+ "values(?,?,?,?,?,?,?,?)";
		Object[] params=new Object[]{sessionAggrAtat.getTask_id(),
				sessionAggrAtat.getSession_count(),
				sessionAggrAtat.getVisti_0s_3s(),
				sessionAggrAtat.getVisti_4s_6s(),
				sessionAggrAtat.getVisti_10s_30s(),
				sessionAggrAtat.getStep_0_30(),
				sessionAggrAtat.getStep_30_60(),
				sessionAggrAtat.getStep_61()};
		DBUtil.getInstance().update(sql, params);
		
	}

	public void insertSessionRandomExtract(SessionRandomExtract sessionRandomExtract) {
		String sql="insert into session_random_extract(task_id,session_id,start_time,end_time,search_keywords) values(?,?,?,?,?)";

		Object[] params=new Object[]{sessionRandomExtract.getTask_id(),
				sessionRandomExtract.getSession_id(),
				sessionRandomExtract.getStart_time(),
				sessionRandomExtract.getEnd_time(),
				sessionRandomExtract.getSearch_keywords()};
		DBUtil.getInstance().update(sql, params);
	}

	public void insertSessionDetail(SessionDetail sessionDetail) {
		String sql="insert into session_detail(task_id,user_id,session_id,page_id,action_time,search_keyword,click_category_id,click_product_id,order_category_ids,order_product_ids,pay_category_ids,pay_product_ids) values(?,?,?,?,?,?,?,?,?,?,?,?)";
		Object[] params=new Object[]{sessionDetail.getTask_id(),
				sessionDetail.getUser_id(),
				sessionDetail.getSession_id(),
				sessionDetail.getPage_id(),
				sessionDetail.getAction_time(),
				sessionDetail.getSearch_keyword(),
				sessionDetail.getClick_category_id(),
				sessionDetail.getClick_product_id(),
				sessionDetail.getOrder_category_id(),
				sessionDetail.getOrder_product_ids(),
				sessionDetail.getPay_category_ids(),
				sessionDetail.getPay_product_ids()};
		DBUtil.getInstance().update(sql, params);
		
		
	}
	
	
/*	public static void main(String[] args) {
		TaskDao taskFactory = Factory.getTaskFactory();
		Task findById = taskFactory.findById(1L);
		String taskParam = findById.getTask_param();//[{"sex":["female"]}]
		
		//json字符串，解析为对象
		JSONArray parseArray = JSONArray.parseArray(taskParam);//json数组对象
		JSONObject jsonObject = parseArray.getJSONObject(0);//获取第一个对象
		JSONArray jsonArray = jsonObject.getJSONArray("sex");//获取第一个对象的sex属性，sex值为数组			
		System.out.println(jsonArray.get(0));//sex值中的第一个值
	
		
		String string = "{'sex':'nan'}";
		JSONObject parseObject = JSONArray.parseObject(string);
		System.out.println(parseObject.getString("sex"));
		
		//对象转换为json字符串
		Task task = new Task();
		task.setTask_name("aa");
		String jsonString = JSONArray.toJSONString(task);
		System.out.println(jsonString);
	}*/

}


