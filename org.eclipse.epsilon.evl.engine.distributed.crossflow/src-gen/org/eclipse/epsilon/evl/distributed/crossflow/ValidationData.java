package org.eclipse.epsilon.evl.distributed.crossflow;

import javax.annotation.Generated;
import java.io.Serializable;
import java.util.Collection;
import java.util.ArrayList;
import org.eclipse.scava.crossflow.runtime.Job;

@Generated(value = "org.eclipse.scava.crossflow.java.Type2Class", date = "2019-11-30T17:04:27.022703400Z")
public class ValidationData extends Job {
		
	protected java.io.Serializable data;
	
	/**
	 * Default Constructor
	 * <p>
	 * Deserialization requires an empty instance to modify
	 * </p>
	 */
	public ValidationData() {
		;
	}
	
	/**
	 * Constructor allow initialization of all declared fields
	 */
	public ValidationData(java.io.Serializable data) {
		this.data = data;
	}
	
	 
	public ValidationData(java.io.Serializable data, Job correlation) {
		this.data = data;
		this.correlationId = correlation.getCorrelationId();
	}
	
	public java.io.Serializable getData() {
		return this.data;
	}
	
	public void setData(java.io.Serializable data) {
		this.data = data;
	}
	
	public Object[] toObjectArray() {
		Object[] ret = new Object[1];
	 	ret[0] = getData();
		return ret;
	}
	
	public String toString() {
		return "ValidationData (" + " data=" + data + " jobId=" + jobId + " correlationId=" + correlationId + " destination=" + destination + ")";
	}
	
}

