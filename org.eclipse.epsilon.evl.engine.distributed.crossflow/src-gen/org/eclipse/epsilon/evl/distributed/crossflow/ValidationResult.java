package org.eclipse.epsilon.evl.distributed.crossflow;

import javax.annotation.Generated;
import java.io.Serializable;
import java.util.Collection;
import java.util.ArrayList;
import org.eclipse.scava.crossflow.runtime.Job;

@Generated(value = "org.eclipse.scava.crossflow.java.Type2Class", date = "2019-10-18T14:16:53.865523500+01:00[Europe/London]")
public class ValidationResult extends Job {
	
	public ValidationResult() {}
	
	public ValidationResult(Collection<java.io.Serializable> atoms) {
		this.atoms = atoms;
	}
	
	public ValidationResult(Collection<java.io.Serializable> atoms, Job correlation) {
		this.atoms = atoms;
		this.correlationId = correlation.getId();
	}
		
	protected Collection<java.io.Serializable> atoms = new ArrayList<>();
	
	public void setAtoms(Collection<java.io.Serializable> atoms) {
		this.atoms = atoms;
	}
	
	public Collection<java.io.Serializable> getAtoms() {
		return atoms;
	}
	
	
	public Object[] toObjectArray() {
		Object[] ret = new Object[1];
	 	ret[0] = getAtoms();
		return ret;
	}
	
	public String toString() {
		return "ValidationResult (" + " atoms=" + atoms + " id=" + id + " correlationId=" + correlationId + " destination=" + destination + ")";
	}
	
}

