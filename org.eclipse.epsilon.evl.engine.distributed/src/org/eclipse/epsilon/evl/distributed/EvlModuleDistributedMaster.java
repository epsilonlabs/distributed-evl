/*********************************************************************
 * Copyright (c) 2018 The University of York.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
**********************************************************************/
package org.eclipse.epsilon.evl.distributed;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;
import org.eclipse.epsilon.eol.exceptions.EolRuntimeException;
import org.eclipse.epsilon.eol.function.CheckedEolRunnable;
import org.eclipse.epsilon.evl.distributed.execute.context.EvlContextDistributedMaster;
import org.eclipse.epsilon.evl.distributed.execute.data.*;
import org.eclipse.epsilon.evl.distributed.strategy.JobSplitter;

/**
 * Base implementation of EVL with distributed execution semantics.
 * Splitting is supported at the element-level granularity. The {@link #checkConstraints()}
 * method initiates the distributed processing; which in turn should spawn instances of
 * {@link EvlModuleDistributedSlave}. If a data sink is used (i.e.the results can be
 * acquired by this module as they appear), the 
 * {@link SerializableEvlResultAtom#deserializeEager(org.eclipse.epsilon.evl.IEvlModule)} 
 * method can be used to rebuild the unsatisfied constraints and apply them to the context. Otherwise if
 * the processing is blocking (i.e. the master must wait for all results to become available), then
 * {@linkplain #assignDeserializedResults(Stream)} can be used.
 * 
 * @see {@link EvlModuleDistributedSlave}
 * @author Sina Madani
 * @since 1.6
 */
public abstract class EvlModuleDistributedMaster extends EvlModuleDistributed {
	
	public EvlModuleDistributedMaster(EvlContextDistributedMaster context) {
		super(Objects.requireNonNull(context));
	}
	
	/**
	 * This method is called asynchronously from {@link #checkConstraints()}.
	 * 
	 * @param jobs The jobs to execute locally using this module.
	 * @throws EolRuntimeException
	 */
	protected void executeMasterJobs(Collection<?> jobs) throws EolRuntimeException {
		getContext().executeJob(jobs);
	}
	
	/**
	 * This method is called asynchronously from {@link #checkConstraints()}.
	 * 
	 * @param jobs The jobs to distribute to workers.
	 * @throws EolRuntimeException
	 */
	protected abstract void executeWorkerJobs(Collection<? extends Serializable> jobs) throws EolRuntimeException;
	
	@Override
	protected final void checkConstraints() throws EolRuntimeException {
		JobSplitter splitter = getContext().getJobSplitter();
		splitter.split(getAllJobs());
		Collection<?> masterJobs = splitter.getMasterJobs();
		Collection<? extends Serializable> workerJobs = splitter.getWorkerJobs();
		
		if (masterJobs.isEmpty() && workerJobs.isEmpty()) {
			return;
		}
		else if (masterJobs.isEmpty() && !workerJobs.isEmpty()) {
			executeWorkerJobs(workerJobs);
		}
		else if (!masterJobs.isEmpty() && workerJobs.isEmpty()) {
			executeMasterJobs(masterJobs);
		}
		else {
			CheckedEolRunnable masterAsync = () -> executeMasterJobs(masterJobs);
			CheckedEolRunnable workerAsync = () -> executeWorkerJobs(workerJobs);
			try {
				CompletableFuture.runAsync(masterAsync)
					.thenCombine(
						CompletableFuture.runAsync(workerAsync),
						(v1, v2) -> null
					)
				.get();
			}
			catch (InterruptedException | ExecutionException ex) {
				ex.printStackTrace();
				EolRuntimeException.propagateDetailed(ex);
			}
		}
	}
	
	@Override
	protected void prepareContext() throws EolRuntimeException {
		getContext().storeInitialVariables();
		super.prepareContext();
	}
	
	@Override
	public EvlContextDistributedMaster getContext() {
		return (EvlContextDistributedMaster) super.getContext();
	}
	
}
