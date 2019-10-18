/*********************************************************************
 * Copyright (c) 2019 The University of York.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
**********************************************************************/
package org.eclipse.epsilon.evl.distributed.crossflow;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import org.eclipse.epsilon.evl.distributed.crossflow.launch.CrossflowEvlRunConfigurationMaster;
import org.eclipse.epsilon.evl.distributed.launch.DistributedEvlRunConfigurationSlave;

/**
 * 
 * @author Sina Madani
 * @since 1.6
 */
public class Processing extends ProcessingBase {
	
	boolean isMaster;
	DistributedEvlRunConfigurationSlave configuration;
	
	@SuppressWarnings("unchecked")
	@Override
	public void consumeConfigConfigTopic(Config config) throws Exception {
		if (isMaster = workflow.isMaster()) return;
		
		assert workflow.isWorker();
		while (configuration == null) synchronized (this) {
			configuration = DistributedEvlRunConfigurationSlave.parseJobParameters(
				(Map<String, ? extends Serializable>) config.data,
				System.getProperty(CrossflowEvlRunConfigurationMaster.BASE_PATH_SYSTEM_PROPERTY)
			);
			notify();
		}
	}
	
	@Override
	public void consumeValidationDataQueue(ValidationData validationData) throws Exception {
		while (!isMaster && configuration == null) synchronized (this) {
			wait();
		}
		
		final java.io.Serializable job = validationData.data;
		
		if (isMaster) {
			workflow.configConfigSource.module.getContext().executeJob(job);
		}
		else {
			assert workflow.isWorker() && configuration != null;
			Collection<Serializable> results = configuration.getModule().getContext().executeJobStateless(job);
			if (results != null) {
				sendToValidationOutput(new ValidationResult(results));
			}
		}
	}
}