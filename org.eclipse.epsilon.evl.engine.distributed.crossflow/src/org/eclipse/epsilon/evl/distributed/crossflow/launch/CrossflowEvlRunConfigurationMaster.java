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
import org.eclipse.epsilon.evl.distributed.launch.DistributedEvlRunConfiguration;
import org.eclipse.epsilon.evl.distributed.launch.DistributedEvlRunConfigurationMaster;

/**
 * 
 * @author Sina Madani
 * @since 1.6
 */
public class CrossflowEvlRunConfigurationMaster extends DistributedEvlRunConfigurationMaster {
	
	public static class Builder<R extends CrossflowEvlRunConfigurationMaster, B extends Builder<R, B>> extends DistributedEvlRunConfigurationMaster.Builder<R, B> {
		
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
	
	public CrossflowEvlRunConfigurationMaster(Builder<? extends DistributedEvlRunConfiguration, ?> builder) {
		super(builder);
	}

	@Override
	protected EvlModuleCrossflowMaster getDefaultModule() {
		return new EvlModuleCrossflowMaster();
	}
}
