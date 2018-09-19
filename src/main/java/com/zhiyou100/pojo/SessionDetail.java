package com.zhiyou100.pojo;

import java.io.Serializable;

public class SessionDetail implements Serializable{

	private Integer task_id;
	private Integer user_id;
	private String session_id;
	private Integer page_id;
	private String action_time;
	private String search_keyword;
	private Integer click_category_id;
	private Integer click_product_id;
	private String order_category_id;
	private String order_product_ids;
	private String pay_category_ids;
	private String pay_product_ids;
	public SessionDetail() {
		super();
	}
	public SessionDetail(Integer task_id, Integer user_id, String session_id, Integer page_id, String action_time,
			String search_keyword, Integer click_category_id, Integer click_product_id, String order_category_id,
			String order_product_ids, String pay_category_ids, String pay_product_ids) {
		super();
		this.task_id = task_id;
		this.user_id = user_id;
		this.session_id = session_id;
		this.page_id = page_id;
		this.action_time = action_time;
		this.search_keyword = search_keyword;
		this.click_category_id = click_category_id;
		this.click_product_id = click_product_id;
		this.order_category_id = order_category_id;
		this.order_product_ids = order_product_ids;
		this.pay_category_ids = pay_category_ids;
		this.pay_product_ids = pay_product_ids;
	}
	@Override
	public String toString() {
		return "SessionDetail [task_id=" + task_id + ", user_id=" + user_id + ", session_id=" + session_id
				+ ", page_id=" + page_id + ", action_time=" + action_time + ", search_keyword=" + search_keyword
				+ ", click_category_id=" + click_category_id + ", click_product_id=" + click_product_id
				+ ", order_category_id=" + order_category_id + ", order_product_ids=" + order_product_ids
				+ ", pay_category_ids=" + pay_category_ids + ", pay_product_ids=" + pay_product_ids + "]";
	}
	public SessionDetail(Integer user_id, String session_id, Integer page_id, String action_time, String search_keyword,
			Integer click_category_id, Integer click_product_id, String order_category_id, String order_product_ids,
			String pay_category_ids, String pay_product_ids) {
		super();
		this.user_id = user_id;
		this.session_id = session_id;
		this.page_id = page_id;
		this.action_time = action_time;
		this.search_keyword = search_keyword;
		this.click_category_id = click_category_id;
		this.click_product_id = click_product_id;
		this.order_category_id = order_category_id;
		this.order_product_ids = order_product_ids;
		this.pay_category_ids = pay_category_ids;
		this.pay_product_ids = pay_product_ids;
	}
	public Integer getTask_id() {
		return task_id;
	}
	public void setTask_id(Integer task_id) {
		this.task_id = task_id;
	}
	public Integer getUser_id() {
		return user_id;
	}
	public void setUser_id(Integer user_id) {
		this.user_id = user_id;
	}
	public String getSession_id() {
		return session_id;
	}
	public void setSession_id(String session_id) {
		this.session_id = session_id;
	}
	public Integer getPage_id() {
		return page_id;
	}
	public void setPage_id(Integer page_id) {
		this.page_id = page_id;
	}
	public String getAction_time() {
		return action_time;
	}
	public void setAction_time(String action_time) {
		this.action_time = action_time;
	}
	public String getSearch_keyword() {
		return search_keyword;
	}
	public void setSearch_keyword(String search_keyword) {
		this.search_keyword = search_keyword;
	}
	public Integer getClick_category_id() {
		return click_category_id;
	}
	public void setClick_category_id(Integer click_category_id) {
		this.click_category_id = click_category_id;
	}
	public Integer getClick_product_id() {
		return click_product_id;
	}
	public void setClick_product_id(Integer click_product_id) {
		this.click_product_id = click_product_id;
	}
	public String getOrder_category_id() {
		return order_category_id;
	}
	public void setOrder_category_id(String order_category_id) {
		this.order_category_id = order_category_id;
	}
	public String getOrder_product_ids() {
		return order_product_ids;
	}
	public void setOrder_product_ids(String order_product_ids) {
		this.order_product_ids = order_product_ids;
	}
	public String getPay_category_ids() {
		return pay_category_ids;
	}
	public void setPay_category_ids(String pay_category_ids) {
		this.pay_category_ids = pay_category_ids;
	}
	public String getPay_product_ids() {
		return pay_product_ids;
	}
	public void setPay_product_ids(String pay_product_ids) {
		this.pay_product_ids = pay_product_ids;
	}
	
	
	
	
}
