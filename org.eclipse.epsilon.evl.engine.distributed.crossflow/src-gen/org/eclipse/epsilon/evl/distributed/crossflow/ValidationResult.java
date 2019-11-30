package org.eclipse.epsilon.evl.distributed.crossflow;

import javax.annotation.Generated;
import java.io.Serializable;
import java.util.Collection;
import java.util.ArrayList;
import org.eclipse.scava.crossflow.runtime.Job;

@Generated(value = "org.eclipse.scava.crossflow.java.Type2Class", date = "2019-11-30T17:04:27.022703400Z")
public class ValidationResult extends Job {
		
	protected Collection<java.io.Serializable> atoms = new ArrayList<java.io.Serializable>();
	
	/**
	 * Default Constructor
	 * <p>
	 * Deserialization requires an empty instance to modify
	 * </p>
	 */
	public ValidationResult() {
		;
	}
	
	/**
	 * Constructor allow initialization of all declared fields
	 */
	public ValidationResult(Collection<java.io.Serializable> atoms) {
		this.atoms = atoms;
	}
	
	 
	public ValidationResult(Collection<java.io.Serializable> atoms, Job correlation) {
		this.atoms = atoms;
		this.correlationId = correlation.getCorrelationId();
	}
	
	public Collection<java.io.Serializable> getAtoms() {
		return this.atoms;
	}
	
	public void setAtoms(Collection<java.io.Serializable> atoms) {
		this.atoms = atoms;
	}
	
	public Object[] toObjectArray() {
		Object[] ret = new Object[1];
	 	ret[0] = getAtoms();
		return ret;
	}
	
	public String toString() {
		return "ValidationResult (" + " atoms=" + atoms + " jobId=" + jobId + " correlationId=" + correlationId + " destination=" + destination + ")";
	}
	
}

