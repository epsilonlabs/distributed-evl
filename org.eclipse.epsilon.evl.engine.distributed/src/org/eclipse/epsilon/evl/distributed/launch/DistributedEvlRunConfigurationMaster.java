/*********************************************************************
 * Copyright (c) 2019 The University of York.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
**********************************************************************/
package org.eclipse.epsilon.evl.distributed.launch;

import org.eclipse.epsilon.evl.distributed.EvlModuleDistributedMaster;
import org.eclipse.epsilon.evl.distributed.execute.context.EvlContextDistributedMaster;
import org.eclipse.epsilon.evl.distributed.strategy.JobSplitter;

/**
 * 
 * @author Sina Madani
 * @since 1.6
 */
public abstract class DistributedEvlRunConfigurationMaster extends DistributedEvlRunConfiguration {

	@SuppressWarnings("unchecked")
	public static abstract class Builder<R extends DistributedEvlRunConfigurationMaster, B extends Builder<R, B>> extends DistributedEvlRunConfiguration.Builder<R, B> {
		public static final double UNINTIALIZED_VALUE = -Double.MAX_VALUE;
		
		JobSplitter jobSplitter;
		public int distributedParallelism;
		public boolean shuffle = true;
		public double
			batchFactor = UNINTIALIZED_VALUE,
			masterProportion = UNINTIALIZED_VALUE;
		
		public B withDistributedParallelism(int workers) {
			this.distributedParallelism = workers;
			return (B) this;
		}
		public B withBatchFactor(double bf) {
			this.batchFactor = bf;
			return (B) this;
		}
		public B noShuffle() {
			return withShuffle(false);
		}
		public B withShuffle(boolean shuffle) {
			this.shuffle = shuffle;
			return (B) this;
		}
		public B withJobSplitter(JobSplitter splitter) {
			this.jobSplitter = splitter;
			return (B) this;
		}
		
		@Override
		protected abstract EvlModuleDistributedMaster createModule();
		
		public JobSplitter getJobSplitter() {
			if (jobSplitter == null) {
				jobSplitter = new JobSplitter(shuffle, masterProportion, batchFactor);
			}
			return jobSplitter;
		}
		
		@Override
		protected void preBuild() {
			super.preBuild();
			getJobSplitter();
		}
		
		protected Builder() {
			super();
		}
		protected Builder(Class<R> runConfigClass) {
			super(runConfigClass);
		}
	}
	
	protected final int distributedParallelism;
	protected final JobSplitter jobSplitter;
	
	public DistributedEvlRunConfigurationMaster(DistributedEvlRunConfigurationMaster other) {
		super(other);
		this.distributedParallelism = other.distributedParallelism;
		this.jobSplitter = other.jobSplitter;
	}
	
	public DistributedEvlRunConfigurationMaster(Builder<? extends DistributedEvlRunConfiguration, ?> builder) {
		super(builder);
		this.distributedParallelism = builder.distributedParallelism;
		this.jobSplitter = builder.jobSplitter;
		EvlContextDistributedMaster context = getModule().getContext();
		context.setModelProperties(modelsAndProperties.values());
		context.setBasePath(basePath);
		if (outputFile != null) {
			context.setOutputPath(outputFile.toString());
		}
	}
	
	@Override
	public EvlModuleDistributedMaster getModule() {
		return (EvlModuleDistributedMaster) super.getModule();
	}
}
