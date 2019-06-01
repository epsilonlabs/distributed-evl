/*********************************************************************
 * Copyright (c) 2019 The University of York.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
**********************************************************************/
package org.eclipse.epsilon.evl.distributed.jms.atomic;

import java.io.Serializable;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.eclipse.epsilon.evl.distributed.execute.context.EvlContextDistributedMaster;
import org.eclipse.epsilon.evl.distributed.execute.data.SerializableEvlInputParametersAtom;
import org.eclipse.epsilon.evl.distributed.jms.EvlModuleJmsMaster;

/**
 * 
 *
 * @author Sina Madani
 * @since 1.6
 */
public class EvlModuleJmsMasterParameters extends EvlModuleJmsMaster {
	
	public EvlModuleJmsMasterParameters(int expectedWorkers, String host, int sessionID) throws URISyntaxException {
		super(expectedWorkers, host, sessionID);
		
		EvlContextDistributedMaster context = getContext();
		context = new EvlContextDistributedMaster(context.getParallelism(), context.getDistributedParallelism()) {
			@Override
			public HashMap<String, Serializable> getJobParameters(boolean stripBasePath) {
				HashMap<String, Serializable> jobParams = super.getJobParameters(stripBasePath);
				jobParams.put(IGNORE_MODELS, true);
				return jobParams;
			}
		};
		setContext(context);
	}
	
	@Override
	protected void processJobs(AtomicInteger workersReady) throws Exception {
		waitForWorkersToConnect(workersReady);
		for (SerializableEvlInputParametersAtom job : SerializableEvlInputParametersAtom.createJobs(this)) {
			sendJob(job);
		}
		signalCompletion();
		log("Finished sending jobs to workers");
	}

}
