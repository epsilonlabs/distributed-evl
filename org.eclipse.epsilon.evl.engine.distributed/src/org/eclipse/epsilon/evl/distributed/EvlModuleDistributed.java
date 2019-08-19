/*********************************************************************
 * Copyright (c) 2019 The University of York.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
**********************************************************************/
package org.eclipse.epsilon.evl.distributed;

import java.util.*;
import java.util.stream.*;
import org.eclipse.epsilon.common.concurrent.ConcurrencyUtils;
import org.eclipse.epsilon.eol.exceptions.EolRuntimeException;
import org.eclipse.epsilon.eol.execute.context.IEolContext;
import org.eclipse.epsilon.evl.concurrent.atomic.EvlModuleParallelContextAtoms;
import org.eclipse.epsilon.evl.distributed.execute.context.EvlContextDistributed;
import org.eclipse.epsilon.evl.distributed.execute.data.*;
import org.eclipse.epsilon.evl.execute.UnsatisfiedConstraint;

/**
 * 
 *
 * @author Sina Madani
 * @since 1.6
 */
public abstract class EvlModuleDistributed extends EvlModuleParallelContextAtoms {

	public EvlModuleDistributed(EvlContextDistributed context) {
		super(context);
	}
	
	/**
	 * Executes the provided Serializable job(s) and returns the Serializable result.
	 * 
	 * @param job The Serializable input job(s).
	 * @return A Serializable Collection containing zero or more {@link SerializableEvlResultAtom}s,
	 * or <code>null</code> if this module is the master.
	 * @throws EolRuntimeException If an exception occurs when executing the job using this module.
	 * @throws IllegalArgumentException If the job type was not recognised.
	 */
	@Override
	public final Collection<SerializableEvlResultAtom> executeJob(Object job) throws EolRuntimeException {
		if (job == null) {
			return null;
		}
		else if (job instanceof SerializableEvlResultAtom) {
			return Collections.singletonList((SerializableEvlResultAtom) job);
		}
		
		final EvlContextDistributed context = getContext();
		final Set<UnsatisfiedConstraint>
			originalUc = context.getUnsatisfiedConstraints(),
			tempUc = ConcurrencyUtils.concurrentSet(16, context.getParallelism());
		context.setUnsatisfiedConstraints(tempUc);
		
		try {
			super.executeJob(job);
			return serializeResults(tempUc);
		}
		finally {
			originalUc.addAll(tempUc);
			context.setUnsatisfiedConstraints(originalUc);
		}
	}
	
	protected Collection<SerializableEvlResultAtom> serializeResults(Collection<UnsatisfiedConstraint> unsatisfiedConstraints) {
		EvlContextDistributed context = getContext();
		return unsatisfiedConstraints.parallelStream()
			.map(uc -> SerializableEvlResultAtom.serializeResult(uc, context))
			.collect(Collectors.toCollection(ArrayList::new));
	}
	
	@Override
	public EvlContextDistributed getContext() {
		return (EvlContextDistributed) super.getContext();
	}
	
	@Override
	public void setContext(IEolContext context) {
		if (context instanceof EvlContextDistributed) {
			super.setContext(context);
		}
		else if (context != null) {
			throw new IllegalArgumentException(
				"Invalid context type: expected "+EvlContextDistributed.class.getName()
				+ " but got "+context.getClass().getName()
			);
		}
	}
}
