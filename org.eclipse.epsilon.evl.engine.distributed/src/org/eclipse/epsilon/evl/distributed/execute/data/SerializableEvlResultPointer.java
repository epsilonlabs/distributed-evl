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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import org.eclipse.epsilon.eol.exceptions.EolRuntimeException;
import org.eclipse.epsilon.erl.execute.data.JobBatch;
import org.eclipse.epsilon.evl.concurrent.atomic.EvlModuleParallelContextAtoms;
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
	public String[] constraintNames;
	
	public SerializableEvlResultPointer(int position, Constraint... constraints) {
		this.index = position;
		if (constraints != null && constraints.length > 0) {
			constraintNames = new String[constraints.length];
			int i = 0;
			for (Constraint constraint : constraints) {
				constraintNames[i++] = constraint.getName();
			}
		}
		else {
			constraintNames = null;
		}
	}
	
	@Override
	public int hashCode() {
		return Objects.hash(index, Arrays.hashCode(constraintNames));
	}
	
	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (!(o instanceof SerializableEvlResultPointer)) return false;
		SerializableEvlResultPointer other = (SerializableEvlResultPointer) o;
		return this.index == other.index &&
			Arrays.equals(this.constraintNames, other.constraintNames);
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
		clone.constraintNames = this.constraintNames;
		return clone;
	}
	
	@Override
	public String toString() {
		return getClass().getSimpleName()+" {index="+index+
			", constraintNames="+ Arrays.toString(constraintNames)+"}";
	}
	
	/**
	 * Resolves the UnsatisfiedConstraints based on the job list from the provided module
	 * for this object's index and constraints.
	 * 
	 * @param module
	 * @return
	 * @throws EolRuntimeException
	 */
	public Collection<UnsatisfiedConstraint> resolveForModule(EvlModuleParallelContextAtoms module) throws EolRuntimeException {
		if (constraintNames == null || constraintNames.length == 0) return Collections.emptyList();
		ConstraintContextAtom cca = module.getAllJobs().get(index);
		ArrayList<UnsatisfiedConstraint> unsatisfieds = new ArrayList<>(constraintNames.length);
		for (String constraintName : constraintNames) {
			UnsatisfiedConstraint uc = new UnsatisfiedConstraint();
			Constraint constraint = module.getConstraint(constraintName, cca.element, cca.rule);
			uc.setConstraint(constraint);
			uc.setInstance(cca.element);
			uc.setMessage(constraint.getUnsatisfiedMessage(cca.element, module.getContext()));
			unsatisfieds.add(uc);
		}
		return unsatisfieds;
	}
}
