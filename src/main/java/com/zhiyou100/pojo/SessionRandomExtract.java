package com.zhiyou100.pojo;

import java.io.Serializable;

public class SessionRandomExtract implements Serializable{
	private Integer task_id;
	private String session_id;
	private String start_time;
	private String end_time;
	private String search_keywords;
	public SessionRandomExtract() {
		super();
	}
	public SessionRandomExtract(Integer task_id, String session_id, String start_time, String end_time,
			String search_keywords) {
		super();
		this.task_id = task_id;
		this.session_id = session_id;
		this.start_time = start_time;
		this.end_time = end_time;
		this.search_keywords = search_keywords;
	}
	public SessionRandomExtract(String session_id, String start_time, String end_time, String search_keywords) {
		super();
		this.session_id = session_id;
		this.start_time = start_time;
		this.end_time = end_time;
		this.search_keywords = search_keywords;
	}
	public Integer getTask_id() {
		return task_id;
	}
	public void setTask_id(Integer task_id) {
		this.task_id = task_id;
	}
	public String getSession_id() {
		return session_id;
	}
	public void setSession_id(String session_id) {
		this.session_id = session_id;
	}
	public String getStart_time() {
		return start_time;
	}
	public void setStart_time(String start_time) {
		this.start_time = start_time;
	}
	public String getEnd_time() {
		return end_time;
	}
	public void setEnd_time(String end_time) {
		this.end_time = end_time;
	}
	public String getSearch_keywords() {
		return search_keywords;
	}
	public void setSearch_keywords(String search_keywords) {
		this.search_keywords = search_keywords;
	}
	@Override
	public String toString() {
		return "SessionRandomExtractDao [task_id=" + task_id + ", session_id=" + session_id + ", start_time=" + start_time
				+ ", end_time=" + end_time + ", search_keywords=" + search_keywords + "]";
	}
	
	

}
