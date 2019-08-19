/*********************************************************************
 * Copyright (c) 2019 The University of York.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
**********************************************************************/
package org.eclipse.epsilon.evl.distributed.jms.launch;

import org.eclipse.epsilon.evl.distributed.jms.EvlModuleJmsMaster;
import org.eclipse.epsilon.evl.distributed.jms.batch.EvlModuleJmsMasterBatch;
import org.eclipse.epsilon.evl.distributed.jms.execute.context.EvlContextJmsMaster;
import org.eclipse.epsilon.evl.distributed.launch.DistributedEvlRunConfiguration;
import org.eclipse.epsilon.evl.distributed.launch.DistributedEvlRunConfigurationMaster;
import org.eclipse.epsilon.evl.distributed.strategy.BatchJobSplitter;

/**
 * 
 *
 * @author Sina Madani
 * @since 1.6
 */
public class JmsEvlRunConfigurationMaster extends DistributedEvlRunConfigurationMaster {

	public static class Builder<R extends JmsEvlRunConfigurationMaster, B extends Builder<R, B>> extends DistributedEvlRunConfigurationMaster.Builder<R, B> {
		@Override
		protected EvlModuleJmsMaster createModule() {
			EvlContextJmsMaster context = new EvlContextJmsMaster(parallelism, distributedParallelism, host, sessionID);
			BatchJobSplitter strategy = new BatchJobSplitter(context, masterProportion, shuffle, batchFactor);
			return new EvlModuleJmsMasterBatch(context, strategy);
		}
		
		protected Builder() {
			super();
		}
		protected Builder(Class<R> runConfigClass) {
			super(runConfigClass);
		}
	}
	
	public JmsEvlRunConfigurationMaster(DistributedEvlRunConfigurationMaster other) {
		super(other);
	}

	public JmsEvlRunConfigurationMaster(Builder<? extends DistributedEvlRunConfiguration, ?> builder) {
		super(builder);
	}
}
