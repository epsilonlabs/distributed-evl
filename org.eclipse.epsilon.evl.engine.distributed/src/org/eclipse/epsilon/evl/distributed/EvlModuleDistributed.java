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
import org.eclipse.epsilon.eol.execute.concurrent.executors.EolExecutorService;
import org.eclipse.epsilon.eol.execute.context.IEolContext;
import org.eclipse.epsilon.erl.execute.data.JobBatch;
import org.eclipse.epsilon.evl.concurrent.EvlModuleParallel;
import org.eclipse.epsilon.evl.distributed.execute.context.EvlContextDistributed;
import org.eclipse.epsilon.evl.distributed.execute.data.*;
import org.eclipse.epsilon.evl.execute.UnsatisfiedConstraint;
import org.eclipse.epsilon.evl.execute.atoms.*;

/**
 * 
 *
 * @author Sina Madani
 * @since 1.6
 */
public abstract class EvlModuleDistributed extends EvlModuleParallel {

	public EvlModuleDistributed(int distributedParallelism) {
		super(distributedParallelism);
		setContext(new EvlContextDistributed(distributedParallelism));
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
			executeJobImpl(job, false);
			return serializeResults(tempUc);
		}
		finally {
			originalUc.addAll(tempUc);
			context.setUnsatisfiedConstraints(originalUc);
		}
	}

	/**
	 * Evaluates the job locally, adding the results to the Set of UnsatisfiedConstraint in the context.
	 * 
	 * @param job The job (or jobs) to evaluate.
	 * @param isInLoop Whether this method is being called recursively from a loop.
	 * @throws EolRuntimeException If an exception is thrown whilst evaluating the job(s).
	 */
	protected void executeJobImpl(Object job, boolean isInLoop) throws EolRuntimeException {
		if (job instanceof SerializableEvlInputAtom) {
			((SerializableEvlInputAtom) job).execute(this);
		}
		else if (job instanceof JobBatch) {
			executeJobImpl(((JobBatch) job).split(getContextJobs()), isInLoop);
		}
		else if (job instanceof ConstraintContextAtom) {
			((ConstraintContextAtom) job).execute(getContext());
		}
		else if (job instanceof ConstraintAtom) {
			ConstraintAtom ca = (ConstraintAtom) job;
			ca.unit.execute(ca.element, getContext());
		}
		else if (job instanceof Iterable) {
			executeJobImpl(((Iterable<?>) job).iterator(), isInLoop);
		}
		else if (job instanceof Iterator) {
			EvlContextDistributed context = getContext();
			if (isInLoop) {
				for (
					Iterator<?> iter = (Iterator<?>) job;
					iter.hasNext();
					executeJobImpl(iter.next(), isInLoop)
				);
			}
			else {
				assert context.isParallelisationLegal();
				EolExecutorService executor = context.beginParallelTask();
				for (Iterator<?> iter = (Iterator<?>) job; iter.hasNext();) {
					final Object nextJob = iter.next();
					executor.execute(() -> executeJobImpl(nextJob, true));
				}
				executor.awaitCompletion();
				context.endParallelTask();
			}
		}
		else if (job instanceof BaseStream) {
			executeJobImpl(((BaseStream<?,?>)job).iterator(), isInLoop);
		}
		else if (job instanceof Spliterator) {
			executeJobImpl(StreamSupport.stream(
				(Spliterator<?>) job, getContext().isParallelisationLegal()), isInLoop
			);
		}
		else {
			throw new IllegalArgumentException("Received unexpected object of type "+job.getClass().getName());
		}
	}
	
	List<ConstraintContextAtom> contextJobsCache;
	
	/**
	 * Calls {@link ConstraintContextAtom#getContextJobs(org.eclipse.epsilon.evl.IEvlModule)}
	 * 
	 * @return A cached (re-usable) deterministicly ordered List of jobs.
	 * @throws EolRuntimeException
	 */
	public final List<ConstraintContextAtom> getContextJobs() throws EolRuntimeException {
		if (contextJobsCache == null) {
			contextJobsCache = ConstraintContextAtom.getContextJobs(this);
		}
		return contextJobsCache;
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
	}
}
