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

import org.eclipse.epsilon.eol.exceptions.EolRuntimeException;
import org.eclipse.epsilon.evl.distributed.EvlModuleDistributedMaster;
import org.eclipse.epsilon.evl.distributed.strategy.JobSplitter;
import org.eclipse.epsilon.evl.execute.UnsatisfiedConstraint;

/**
 * 
 * @author Sina Madani
 * @since 1.6
 */
public class EvlContextDistributedMaster extends EvlContextDistributed {

	protected int distributedParallelism;
	protected JobSplitter jobSplitter;
	protected boolean localParallelismExplicitlySpecified;
	
	public EvlContextDistributedMaster(int localParallelism, int distributedParallelism, JobSplitter splitter) {
		super(localParallelism);
		this.localParallelismExplicitlySpecified = localParallelism > 0;
		this.distributedParallelism = distributedParallelism;
		setJobSplitter(splitter);
	}
	
	protected void setJobSplitter(JobSplitter splitter) {
		this.jobSplitter = splitter != null ? splitter : new JobSplitter();
		splitter.setContext(this);
	}
	
	public JobSplitter getJobSplitter() {
		return jobSplitter;
	}
	
	public int getDistributedParallelism() {
		return distributedParallelism;
	}
	
	public void setDistributedParallelism(int parallelism) {
		this.distributedParallelism = parallelism;
	}
	
	/**
	 * Whether this context's local parallelism was set or if it was derived automatically.
	 * 
	 * @return <code>true</code> If the number of threads was excplicitly set on construction.
	 */
	public boolean isLocalParallelismExplicitlySpecified() {
		return localParallelismExplicitlySpecified;
	}
	
	@Override
	public Object executeJob(Object job) throws EolRuntimeException {
		Object result = super.executeJob(job);
		if (result instanceof UnsatisfiedConstraint) {
			getUnsatisfiedConstraints().add((UnsatisfiedConstraint) result);
		}
		return result;
	}
	
	@Override
	public EvlModuleDistributedMaster getModule() {
		return (EvlModuleDistributedMaster) super.getModule();
	}
}
