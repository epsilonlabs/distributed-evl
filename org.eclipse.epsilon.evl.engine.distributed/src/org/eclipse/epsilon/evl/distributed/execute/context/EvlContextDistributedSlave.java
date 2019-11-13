/*********************************************************************
 * Copyright (c) 2018 The University of York.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
**********************************************************************/
package org.eclipse.epsilon.evl.distributed.execute.context;

import java.io.Serializable;
import java.util.*;
import org.eclipse.epsilon.common.concurrent.ConcurrencyUtils;
import org.eclipse.epsilon.eol.exceptions.EolRuntimeException;
import org.eclipse.epsilon.erl.execute.data.JobBatch;
import org.eclipse.epsilon.evl.distributed.EvlModuleDistributedSlave;
import org.eclipse.epsilon.evl.distributed.execute.data.SerializableEvlInputAtom;
import org.eclipse.epsilon.evl.distributed.execute.data.SerializableEvlResultAtom;
import org.eclipse.epsilon.evl.distributed.execute.data.SerializableEvlResultPointer;
import org.eclipse.epsilon.evl.execute.UnsatisfiedConstraint;

/**
 * 
 * @author Sina Madani
 * @since 1.6
 */
public class EvlContextDistributedSlave extends EvlContextDistributed {
	
	public EvlContextDistributedSlave() {
		super();
	}
	
	public EvlContextDistributedSlave(int localParallelism) {
		super(localParallelism);
	}
	
	@Override
	public Set<UnsatisfiedConstraint> getUnsatisfiedConstraints() {
		return unsatisfiedConstraints;
	}
	
	boolean isBatchBased;
	
	@Override
	public Object executeJob(Object job) throws EolRuntimeException {
		if (job instanceof JobBatch) {
			isBatchBased = true;
		}
		else if (job instanceof SerializableEvlInputAtom) {
			isBatchBased = false;
		}
		
		if (job instanceof UnsatisfiedConstraint) {
			UnsatisfiedConstraint uc = (UnsatisfiedConstraint) job;
			return isBatchBased ?
				SerializableEvlResultPointer.serialize(uc, getModule()) :
				SerializableEvlResultAtom.serialize(uc, this);
		}
		
		Object result = super.executeJob(job);
		if (result instanceof UnsatisfiedConstraint) {
			result = executeJob(result);
		}
		return result;
	}
	
	/**
	 * Executes the provided Serializable job(s) and returns the Serializable result.
	 * The context's state (i.e. the UnsatisfiedConstraints) is not modified.
	 * 
	 * @param job The Serializable input job(s).
	 * @return A Serializable Collection containing zero or more {@link SerializableEvlResultAtom}s,
	 * or <code>null</code> if this module is the master.
	 * @throws EolRuntimeException If an exception occurs when executing the job using this module.
	 * @throws IllegalArgumentException If the job type was not recognised.
	 */
	@SuppressWarnings("unchecked")
	public Collection<Serializable> executeJobStateless(Object job) throws EolRuntimeException {
		final Set<UnsatisfiedConstraint>
			originalUc = getUnsatisfiedConstraints(),
			tempUc = ConcurrencyUtils.concurrentSet(16, getParallelism());
		setUnsatisfiedConstraints(tempUc);
		
		try {
			executeJob(job);
			return (Collection<Serializable>) executeJob(tempUc);
		}
		finally {
			setUnsatisfiedConstraints(originalUc);
		}
	}
	
	@Override
	public EvlModuleDistributedSlave getModule() {
		return (EvlModuleDistributedSlave) super.getModule();
	}
}
