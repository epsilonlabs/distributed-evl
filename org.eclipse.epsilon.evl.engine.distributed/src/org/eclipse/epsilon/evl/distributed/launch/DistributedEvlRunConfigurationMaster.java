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

/**
 * 
 * @author Sina Madani
 * @since 1.6
 */
public abstract class DistributedEvlRunConfigurationMaster extends DistributedEvlRunConfiguration {

	@SuppressWarnings("unchecked")
	public static class Builder<R extends DistributedEvlRunConfigurationMaster, B extends Builder<R, B>> extends DistributedEvlRunConfiguration.Builder<R, B> {
		
		public int distributedParallelism;
		public double batchFactor = -1, masterProportion = -1;
		public boolean shuffle;
		
		public B withDistributedParallelism(int workers) {
			this.distributedParallelism = workers;
			return (B) this;
		}
		public B withBatchFactor(double bf) {
			this.batchFactor = bf;
			return (B) this;
		}
		public B shuffle() {
			return withShuffle(true);
		}
		public B withShuffle(boolean shuffle) {
			this.shuffle = shuffle;
			return (B) this;
		}
		
		protected Builder() {
			super();
		}
		protected Builder(Class<R> runConfigClass) {
			super(runConfigClass);
		}
	}
	
	protected final int expectedWorkers;
	protected final double batchFactor, masterProportion;
	protected final boolean shuffle;
	
	public DistributedEvlRunConfigurationMaster(DistributedEvlRunConfigurationMaster other) {
		super(other);
		this.shuffle = other.shuffle;
		this.batchFactor = other.batchFactor;
		this.expectedWorkers = other.expectedWorkers;
		this.masterProportion = other.masterProportion;
	}
	
	public DistributedEvlRunConfigurationMaster(Builder<? extends DistributedEvlRunConfiguration, ?> builder) {
		super(builder);
		this.expectedWorkers = builder.distributedParallelism;
		this.shuffle = builder.shuffle;
		this.batchFactor = builder.batchFactor;
		this.masterProportion = builder.masterProportion;
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
	
	@Override
	protected abstract EvlModuleDistributedMaster getDefaultModule();
	
}
