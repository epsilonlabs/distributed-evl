package org.eclipse.epsilon.evl.distributed.crossflow;

import java.io.Serializable;
import java.util.UUID;
import org.eclipse.scava.crossflow.runtime.Job;
import java.util.List;
import java.util.ArrayList;
import java.io.Serializable;

public class Config extends Job {
	
	public Config() {}
	
	public Config(java.util.HashMap data) {
		this.data = data;
	}
	
	public Config(java.util.HashMap data, Job correlation) {
		this.data = data;
		this.correlationId = correlation.getId();
	}
		
	protected java.util.HashMap data;
	
	public void setData(java.util.HashMap data) {
		this.data = data;
	}
	
	public java.util.HashMap getData() {
		return data;
	}
	
	
	public Object[] toObjectArray() {
		Object[] ret = new Object[1];
	 	ret[0] = getData();
		return ret;
	}
	
	public String toString() {
		return "Config (" + " data=" + data + " id=" + id + " correlationId=" + correlationId + " destination=" + destination + ")";
	}
	
}

