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

import java.net.URISyntaxException;
import org.eclipse.epsilon.evl.distributed.jms.EvlModuleJmsMaster;
import org.eclipse.epsilon.evl.distributed.jms.batch.EvlModuleJmsMasterBatch;
import org.eclipse.epsilon.evl.distributed.launch.DistributedEvlRunConfiguration;
import org.eclipse.epsilon.evl.distributed.launch.DistributedEvlRunConfigurationMaster;

/**
 * 
 *
 * @author Sina Madani
 * @since 1.6
 */
public class JmsEvlRunConfigurationMaster extends DistributedEvlRunConfigurationMaster {

	public static class Builder<R extends JmsEvlRunConfigurationMaster, B extends Builder<R, B>> extends DistributedEvlRunConfigurationMaster.Builder<R, B> {
		
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

	@Override
	protected EvlModuleJmsMaster getDefaultModule() {
		try {
			return new EvlModuleJmsMasterBatch(expectedWorkers, masterProportion, batchFactor, shuffle, host, sessionID);
		}
		catch (URISyntaxException ex) {
			throw new IllegalArgumentException(ex);
		}
	}

}
