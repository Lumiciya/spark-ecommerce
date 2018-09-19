package com.zhiyou100.pojo;

import java.io.Serializable;

public class SessionAggrAtat implements Serializable{
	
	private Integer task_id;
	private Integer session_count;
	private Double visti_0s_3s;
	private Double visti_4s_6s;
	private Double visti_10s_30s;
	private Double step_0_30;
	private Double step_30_60;
	private Double step_61;
	public SessionAggrAtat(int task_id, int session_count, double visti_0s_3s, double visti_4s_6s, double visti_10s_30s,
			double step_0_30, double step_30_60, double step_61) {
		super();
		this.task_id = task_id;
		this.session_count = session_count;
		this.visti_0s_3s = visti_0s_3s;
		this.visti_4s_6s = visti_4s_6s;
		this.visti_10s_30s = visti_10s_30s;
		this.step_0_30 = step_0_30;
		this.step_30_60 = step_30_60;
		this.step_61 = step_61;
	}
	public SessionAggrAtat(int session_count, double visti_0s_3s, double visti_4s_6s, double visti_10s_30s,
			double step_0_30, double step_30_60, double step_61) {
		super();
		this.session_count = session_count;
		this.visti_0s_3s = visti_0s_3s;
		this.visti_4s_6s = visti_4s_6s;
		this.visti_10s_30s = visti_10s_30s;
		this.step_0_30 = step_0_30;
		this.step_30_60 = step_30_60;
		this.step_61 = step_61;
	}
	public SessionAggrAtat() {
		super();
	}
	public int getTask_id() {
		return task_id;
	}
	public void setTask_id(int task_id) {
		this.task_id = task_id;
	}
	public int getSession_count() {
		return session_count;
	}
	public void setSession_count(int session_count) {
		this.session_count = session_count;
	}
	public double getVisti_0s_3s() {
		return visti_0s_3s;
	}
	public void setVisti_0s_3s(double visti_0s_3s) {
		this.visti_0s_3s = visti_0s_3s;
	}
	public double getVisti_4s_6s() {
		return visti_4s_6s;
	}
	public void setVisti_4s_6s(double visti_4s_6s) {
		this.visti_4s_6s = visti_4s_6s;
	}
	public double getVisti_10s_30s() {
		return visti_10s_30s;
	}
	public void setVisti_10s_30s(double visti_10s_30s) {
		this.visti_10s_30s = visti_10s_30s;
	}
	public double getStep_0_30() {
		return step_0_30;
	}
	public void setStep_0_30(double step_0_30) {
		this.step_0_30 = step_0_30;
	}
	public double getStep_30_60() {
		return step_30_60;
	}
	public void setStep_30_60(double step_30_60) {
		this.step_30_60 = step_30_60;
	}
	public double getStep_61() {
		return step_61;
	}
	public void setStep_61(double step_61) {
		this.step_61 = step_61;
	}
	@Override
	public String toString() {
		return "SessionAggrAtat [task_id=" + task_id + ", session_count=" + session_count + ", visti_0s_3s="
				+ visti_0s_3s + ", visti_4s_6s=" + visti_4s_6s + ", visti_10s_30s=" + visti_10s_30s + ", step_0_30="
				+ step_0_30 + ", step_30_60=" + step_30_60 + ", step_61=" + step_61 + "]";
	}
	
	
}
