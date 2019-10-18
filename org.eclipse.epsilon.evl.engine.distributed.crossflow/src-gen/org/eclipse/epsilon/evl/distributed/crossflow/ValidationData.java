package org.eclipse.epsilon.evl.distributed.crossflow;

import javax.annotation.Generated;
import java.io.Serializable;
import java.util.Collection;
import java.util.ArrayList;
import org.eclipse.scava.crossflow.runtime.Job;

@Generated(value = "org.eclipse.scava.crossflow.java.Type2Class", date = "2019-10-18T14:16:53.865523500+01:00[Europe/London]")
public class ValidationData extends Job {
	
	public ValidationData() {}
	
	public ValidationData(java.io.Serializable data) {
		this.data = data;
	}
	
	public ValidationData(java.io.Serializable data, Job correlation) {
		this.data = data;
		this.correlationId = correlation.getId();
	}
		
	protected java.io.Serializable data;
	
	public void setData(java.io.Serializable data) {
		this.data = data;
	}
	
	public java.io.Serializable getData() {
		return data;
	}
	
	
	public Object[] toObjectArray() {
		Object[] ret = new Object[1];
	 	ret[0] = getData();
		return ret;
	}
	
	public String toString() {
		return "ValidationData (" + " data=" + data + " id=" + id + " correlationId=" + correlationId + " destination=" + destination + ")";
	}
	
}

