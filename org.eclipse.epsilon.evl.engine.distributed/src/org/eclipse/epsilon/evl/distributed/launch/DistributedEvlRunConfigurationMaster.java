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
import org.eclipse.epsilon.evl.distributed.strategy.*;
/**
 * 
 * @author Sina Madani
 * @since 1.6
 */
public abstract class DistributedEvlRunConfigurationMaster extends DistributedEvlRunConfiguration {

	@SuppressWarnings("unchecked")
	public static abstract class Builder<R extends DistributedEvlRunConfigurationMaster, B extends Builder<R, B>> extends DistributedEvlRunConfiguration.Builder<R, B> {
		public static final double UNINTIALIZED_VALUE = -Double.MAX_VALUE;
		
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
		
		public JobSplitter<?, ?> getStrategy(EvlContextDistributedMaster context) {
			if (batchFactor != UNINTIALIZED_VALUE) {
				return getBatchStrategy(context);
			}
			else if (masterProportion != UNINTIALIZED_VALUE) {
				return getAtomicStrategy(context);
			}
			else {
				return getAnnotationStrategy(context);
			}
		}
		
		protected BatchJobSplitter getBatchStrategy(EvlContextDistributedMaster context) {
			return new BatchJobSplitter(context, masterProportion, shuffle, batchFactor);
		}
		protected AnnotationConstraintAtomJobSplitter getAnnotationStrategy(EvlContextDistributedMaster context) {
			return new AnnotationConstraintAtomJobSplitter(context, shuffle);
		}
		protected ContextAtomJobSplitter getAtomicStrategy(EvlContextDistributedMaster context) {
			return new ContextAtomJobSplitter(context, masterProportion, shuffle);
		}
		
		@Override
		protected abstract EvlModuleDistributedMaster createModule();
		
		protected Builder() {
			super();
		}
		protected Builder(Class<R> runConfigClass) {
			super(runConfigClass);
		}
	}
	
	protected final int distributedParallelism;
	protected final double batchFactor, masterProportion;
	protected final boolean shuffle;
	
	public DistributedEvlRunConfigurationMaster(DistributedEvlRunConfigurationMaster other) {
		super(other);
		this.shuffle = other.shuffle;
		this.batchFactor = other.batchFactor;
		this.distributedParallelism = other.distributedParallelism;
		this.masterProportion = other.masterProportion;
	}
	
	public DistributedEvlRunConfigurationMaster(Builder<? extends DistributedEvlRunConfiguration, ?> builder) {
		super(builder);
		this.distributedParallelism = builder.distributedParallelism;
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
}
