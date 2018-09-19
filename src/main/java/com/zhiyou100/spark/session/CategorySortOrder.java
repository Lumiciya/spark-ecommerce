package com.zhiyou100.spark.session;

import java.io.Serializable;

import scala.math.Ordered;

public class CategorySortOrder implements Ordered<CategorySortOrder>,Serializable {
	private Long clickCount;
	private Long orderCount;
	private Long payCount;
	public CategorySortOrder(Long clickCount, Long orderCount, Long payCount) {
		super();
		this.clickCount = clickCount;
		this.orderCount = orderCount;
		this.payCount = payCount;
	}
	public CategorySortOrder() {
		super();
	}
	public Long getClickCount() {
		return clickCount;
	}
	public void setClickCount(Long clickCount) {
		this.clickCount = clickCount;
	}
	public Long getOrderCount() {
		return orderCount;
	}
	public void setOrderCount(Long orderCount) {
		this.orderCount = orderCount;
	}
	public Long getPayCount() {
		return payCount;
	}
	public void setPayCount(Long payCount) {
		this.payCount = payCount;
	}
	@Override
	public String toString() {
		return "CategorySort [clickCount=" + clickCount + ", orderCount=" + orderCount + ", payCount=" + payCount + "]";
	}
	
	
	
	public boolean $greater(CategorySortOrder arg0) {
		// TODO Auto-generated method stub
		return false;
	}
	public boolean $greater$eq(CategorySortOrder arg0) {
		// TODO Auto-generated method stub
		return false;
	}
	public boolean $less(CategorySortOrder arg0) {
		// TODO Auto-generated method stub
		return false;
	}
	public boolean $less$eq(CategorySortOrder arg0) {
		// TODO Auto-generated method stub
		return false;
	}
	public int compare(CategorySortOrder arg0) {
		// TODO Auto-generated method stub
		return 0;
	}
	public int compareTo(CategorySortOrder arg0) {
		// TODO Auto-generated method stub
		return 0;
	}
}
