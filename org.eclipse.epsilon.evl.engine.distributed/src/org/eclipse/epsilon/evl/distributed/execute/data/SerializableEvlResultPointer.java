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

import java.util.Objects;
import org.eclipse.epsilon.eol.exceptions.EolRuntimeException;
import org.eclipse.epsilon.erl.execute.data.JobBatch;
import org.eclipse.epsilon.evl.concurrent.EvlModuleParallelAtoms;
import org.eclipse.epsilon.evl.distributed.EvlModuleDistributed;
import org.eclipse.epsilon.evl.dom.Constraint;
import org.eclipse.epsilon.evl.execute.UnsatisfiedConstraint;
import org.eclipse.epsilon.evl.execute.atoms.ConstraintContextAtom;
import org.eclipse.epsilon.evl.execute.atoms.EvlAtom;

/**
 * Batch-based reference to an {@link UnsatisfiedConstraint}, relying on deterministic
 * ordering of jobs. Suitable for the {@link EvlModuleParallelContextAtoms} strategy.
 * 
 * @see {@link JobBatch}
 * @author Sina Madani
 * @since 1.6
 */
public class SerializableEvlResultPointer implements SerializableEvlResult, Cloneable {
	
	private static final long serialVersionUID = -4182370285307626160L;
	
	
	public int index;
	public String constraintName;
	
	public <A extends EvlAtom<?>> A getAtom(EvlModuleParallelAtoms<? extends A> module) throws EolRuntimeException {
		return module.getAllJobs().get(index);
	}
	
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

	@Override
	public Constraint getConstraint(EvlModuleDistributed module) throws EolRuntimeException {
		ConstraintContextAtom atom = getAtom(module);
		return module.getConstraint(constraintName, atom.rule.getTypeName(), atom.element, null);
	}

	@Override
	public Object getModelElement(EvlModuleDistributed module) throws EolRuntimeException {
		return getAtom(module).element;
	}

	@Override
	public String getMessage(EvlModuleDistributed module) throws EolRuntimeException {
		return getConstraint(module).getUnsatisfiedMessage(getModelElement(module), module.getContext());
	}

	public static SerializableEvlResult serialize(UnsatisfiedConstraint uc, EvlModuleDistributed module) throws EolRuntimeException {
		if (uc == null) return null;
		SerializableEvlResultPointer serp = new SerializableEvlResultPointer();
		serp.index = module.indexOfModelElement(uc.getInstance());
		Constraint constraint = uc.getConstraint();
		if (constraint != null) serp.constraintName = constraint.getName();
		return serp;
	}
}
