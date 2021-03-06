package com.zhiyou100.pojo;

import java.io.Serializable;

public class Task implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private Long task_id;
	private String task_name;
	private String create_time;
	private String start_time;
	private String finish_time;
	private String task_type;
	private String task_status;
	private String task_param;
	public Task(Long task_id, String task_name, String create_time, String start_time, String finish_time,
			String task_type, String task_status, String task_param) {
		super();
		this.task_id = task_id;
		this.task_name = task_name;
		this.create_time = create_time;
		this.start_time = start_time;
		this.finish_time = finish_time;
		this.task_type = task_type;
		this.task_status = task_status;
		this.task_param = task_param;
	}
	public Task() {
		super();
	}
	public Task(String task_name, String create_time, String start_time, String finish_time, String task_type,
			String task_status, String task_param) {
		super();
		this.task_name = task_name;
		this.create_time = create_time;
		this.start_time = start_time;
		this.finish_time = finish_time;
		this.task_type = task_type;
		this.task_status = task_status;
		this.task_param = task_param;
	}
	public Long getTask_id() {
		return task_id;
	}
	public void setTask_id(Long task_id) {
		this.task_id = task_id;
	}
	public String getTask_name() {
		return task_name;
	}
	public void setTask_name(String task_name) {
		this.task_name = task_name;
	}
	public String getCreate_time() {
		return create_time;
	}
	public void setCreate_time(String create_time) {
		this.create_time = create_time;
	}
	public String getStart_time() {
		return start_time;
	}
	public void setStart_time(String start_time) {
		this.start_time = start_time;
	}
	public String getFinish_time() {
		return finish_time;
	}
	public void setFinish_time(String finish_time) {
		this.finish_time = finish_time;
	}
	public String getTask_type() {
		return task_type;
	}
	public void setTask_type(String task_type) {
		this.task_type = task_type;
	}
	public String getTask_status() {
		return task_status;
	}
	public void setTask_status(String task_status) {
		this.task_status = task_status;
	}
	public String getTask_param() {
		return task_param;
	}
	public void setTask_param(String task_param) {
		this.task_param = task_param;
	}
	@Override
	public String toString() {
		return "Task [task_id=" + task_id + ", task_name=" + task_name + ", create_time=" + create_time
				+ ", start_time=" + start_time + ", finish_time=" + finish_time + ", task_type=" + task_type
				+ ", task_status=" + task_status + ", task_param=" + task_param + "]";
	}
	
	
	
	
	
	
}
