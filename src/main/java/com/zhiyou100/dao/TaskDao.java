package com.zhiyou100.dao;

import com.zhiyou100.pojo.SessionAggrAtat;
import com.zhiyou100.pojo.SessionDetail;
import com.zhiyou100.pojo.SessionRandomExtract;
import com.zhiyou100.pojo.Task;

public interface TaskDao {
	
	//根据id查询
	Task findById(Long id);
	
	void insertSessionAggrAtat(SessionAggrAtat sessionAggrAtat);
	
	void insertSessionRandomExtract(SessionRandomExtract sessionRandomExtract);

	void insertSessionDetail(SessionDetail sessionDetail);
}
