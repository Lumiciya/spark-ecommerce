package com.zhiyou100.factory;

import com.zhiyou100.dao.TaskDao;
import com.zhiyou100.dao.impl.TaskDaoImpl;

//工厂模式
public class Factory {
	
	public static TaskDao getTaskFactory(){
		return new TaskDaoImpl();
	}

}
