/*********************************************************************
 * Copyright (c) 2019 The University of York.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
**********************************************************************/
package org.eclipse.epsilon.evl.distributed.crossflow.launch;

import org.eclipse.epsilon.evl.distributed.crossflow.EvlModuleCrossflowMaster;
import org.eclipse.epsilon.evl.distributed.crossflow.batch.EvlModuleCrossflowMasterBatch;
import org.eclipse.epsilon.evl.distributed.launch.DistributedEvlRunConfigurationMaster;

/**
 * 
 * @author Sina Madani
 * @since 1.6
 */
public class CrossflowEvlRunConfigurationMaster extends DistributedEvlRunConfigurationMaster {
	
	protected final String instanceID;
	
	@SuppressWarnings("unchecked")
	public static class Builder<R extends CrossflowEvlRunConfigurationMaster, B extends Builder<R, B>> extends DistributedEvlRunConfigurationMaster.Builder<R, B> {
		
		public String instanceID = "DistributedEVL";
		
		public B withInstanceID(String id) {
			this.instanceID = id;
			return (B) this;
		}
		
		@Override
		protected R buildInstance() {
			return (R) new CrossflowEvlRunConfigurationMaster(this);
		}
		
		protected Builder() {
			super();
		}
		protected Builder(Class<R> runConfigClass) {
			super(runConfigClass);
		}
	}
	
	public static Builder<CrossflowEvlRunConfigurationMaster, ?> Builder() {
		return new Builder<>(CrossflowEvlRunConfigurationMaster.class);
	}
	
	public CrossflowEvlRunConfigurationMaster(Builder<? extends CrossflowEvlRunConfigurationMaster, ?> builder) {
		super(builder);
		this.instanceID = builder.instanceID;
	}

	@Override
	protected EvlModuleCrossflowMaster getDefaultModule() {
		return new EvlModuleCrossflowMasterBatch(instanceID, expectedWorkers, masterProportion, shuffle, batchFactor);
	}
}
