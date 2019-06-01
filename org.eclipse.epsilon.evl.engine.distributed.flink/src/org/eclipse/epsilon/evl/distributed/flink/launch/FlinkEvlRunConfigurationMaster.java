/*********************************************************************
 * Copyright (c) 2019 The University of York.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
**********************************************************************/
package org.eclipse.epsilon.evl.distributed.flink.launch;

import org.eclipse.epsilon.evl.distributed.flink.EvlModuleFlinkMaster;
import org.eclipse.epsilon.evl.distributed.flink.atomic.EvlModuleFlinkAtoms;
import org.eclipse.epsilon.evl.distributed.launch.DistributedEvlRunConfiguration;
import org.eclipse.epsilon.evl.distributed.launch.DistributedEvlRunConfigurationMaster;

/**
 * 
 * @author Sina Madani
 * @since 1.6
 */
public class FlinkEvlRunConfigurationMaster extends DistributedEvlRunConfigurationMaster {
	
	public static class Builder<R extends FlinkEvlRunConfigurationMaster, B extends Builder<R, B>> extends DistributedEvlRunConfigurationMaster.Builder<R, B> {
		
		protected Builder() {
			super();
		}
		protected Builder(Class<R> runConfigClass) {
			super(runConfigClass);
		}
	}
	
	public static Builder<FlinkEvlRunConfigurationMaster, ?> Builder() {
		return new Builder<>(FlinkEvlRunConfigurationMaster.class);
	}
	
	public FlinkEvlRunConfigurationMaster(Builder<? extends DistributedEvlRunConfiguration, ?> builder) {
		super(builder);
	}

	@Override
	protected EvlModuleFlinkMaster<?> getDefaultModule() {
		return new EvlModuleFlinkAtoms();
	}
}
