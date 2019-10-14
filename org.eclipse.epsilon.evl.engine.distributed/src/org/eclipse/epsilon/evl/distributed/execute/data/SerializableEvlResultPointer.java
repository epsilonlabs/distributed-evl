/*********************************************************************
 * Copyright (c) 2019 The University of York.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
**********************************************************************/
package org.eclipse.epsilon.evl.distributed.execute.data;

import java.io.Serializable;
import java.util.Objects;
import org.eclipse.epsilon.eol.exceptions.EolRuntimeException;
import org.eclipse.epsilon.erl.execute.data.JobBatch;
import org.eclipse.epsilon.evl.distributed.EvlModuleDistributed;
import org.eclipse.epsilon.evl.dom.Constraint;
import org.eclipse.epsilon.evl.execute.UnsatisfiedConstraint;
import org.eclipse.epsilon.evl.execute.atoms.ConstraintContextAtom;

/**
 * Batch-based reference to an {@link UnsatisfiedConstraint}, relying on deterministic
 * ordering of jobs. Suitable for the {@link EvlModuleParallelContextAtoms} strategy.
 * 
 * @see {@link JobBatch}
 * @author Sina Madani
 * @since 1.6
 */
public class SerializableEvlResultPointer implements Serializable, Cloneable {

	private static final long serialVersionUID = 9173981210222106508L;
	
	
	public int index;
	public String constraintName;
	
	@Override
	public int hashCode() {
		return Objects.hash(index, constraintName);
	}
	
	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (!(o instanceof SerializableEvlResultPointer)) return false;
		SerializableEvlResultPointer other = (SerializableEvlResultPointer) o;
		return this.index == other.index &&
			Objects.equals(this.constraintName, other.constraintName);
	}
	
	@Override
	protected SerializableEvlResultPointer clone() {
		SerializableEvlResultPointer clone;
		try {
			clone = (SerializableEvlResultPointer) super.clone();
		}
		catch (CloneNotSupportedException cnsx) {
			throw new UnsupportedOperationException(cnsx);
		}
		clone.index = this.index;
		clone.constraintName = this.constraintName;
		return clone;
	}
	
	@Override
	public String toString() {
		return getClass().getSimpleName()+" {index="+index+
			", constraintName="+constraintName+"}";
	}
	
	/**
	 * Resolves the UnsatisfiedConstraint based on the job list from the provided module
	 * for this object's index and constraint.
	 * 
	 * @param module
	 * @return
	 * @throws EolRuntimeException
	 */
	public UnsatisfiedConstraint deserialize(EvlModuleDistributed module) throws EolRuntimeException {
		if (constraintName == null) return null;
		ConstraintContextAtom cca = module.getAllJobs().get(index);
		UnsatisfiedConstraint uc = new UnsatisfiedConstraint();
		Constraint constraint = module.getConstraint(constraintName, cca.element, cca.rule);
		uc.setConstraint(constraint);
		uc.setInstance(cca.element);
		uc.setMessage(constraint.getUnsatisfiedMessage(cca.element, module.getContext()));
		return uc;
	}
	
	/**
	 * 
	 * @param uc
	 * @param module
	 * @return
	 * @throws EolRuntimeException
	 */
	public static SerializableEvlResultPointer serialize(UnsatisfiedConstraint uc, EvlModuleDistributed module) throws EolRuntimeException {
		if (uc == null) return null;
		SerializableEvlResultPointer serp = new SerializableEvlResultPointer();
		serp.index = module.indexOfModelElement(uc.getInstance());
		Constraint constraint = uc.getConstraint();
		if (constraint != null) serp.constraintName = constraint.getName();
		return serp;
	}
}
