package org.eclipse.epsilon.evl.distributed.crossflow;

import org.eclipse.scava.crossflow.runtime.Job;
import java.io.Serializable;
import java.util.UUID;
import java.util.Collection;
import java.util.Collections;

public class ValidationResult extends Job {
	
	public ValidationResult() {}
	
	public ValidationResult(Collection<org.eclipse.epsilon.evl.distributed.execute.data.SerializableEvlResultAtom> atoms) {
		this.atoms = atoms;
	}
	
	public ValidationResult(Collection<org.eclipse.epsilon.evl.distributed.execute.data.SerializableEvlResultAtom> atoms, Job correlation) {
		this.atoms = atoms;
		this.correlationId = correlation.getId();
	}
		
	protected Collection<org.eclipse.epsilon.evl.distributed.execute.data.SerializableEvlResultAtom> atoms = Collections.emptyList();
	
	public void setAtoms(Collection<org.eclipse.epsilon.evl.distributed.execute.data.SerializableEvlResultAtom> atoms) {
		this.atoms = atoms;
	}
	
	public Collection<org.eclipse.epsilon.evl.distributed.execute.data.SerializableEvlResultAtom> getAtoms() {
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

