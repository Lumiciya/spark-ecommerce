package com.zhiyou100.spark.session;

import java.io.Serializable;

public class CategorySort implements Comparable<CategorySort>,Serializable {
	
	private Long clickCount;
	private Long orderCount;
	private Long payCount;
	public CategorySort(Long clickCount, Long orderCount, Long payCount) {
		super();
		this.clickCount = clickCount;
		this.orderCount = orderCount;
		this.payCount = payCount;
	}
	public CategorySort() {
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
	
	
	//排序的方法
	public int compareTo(CategorySort o) {
		if(this.clickCount>o.clickCount){
			return 1;
		}else if(this.clickCount<o.clickCount){
			return -1;
		}else{
			if(this.orderCount>o.orderCount){
				return 1;
			}else if(this.orderCount<o.orderCount){
				return -1;
			}else{
				if(this.payCount>o.payCount){
					return 1;
				}else if(this.payCount<o.payCount){
					return -1;
				}else{
					return 0;
				}
			}
		}		
	}
	
	
	
	
	
}
