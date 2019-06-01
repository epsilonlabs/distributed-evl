/*********************************************************************
 * Copyright (c) 2019 The University of York.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
**********************************************************************/
package org.eclipse.epsilon.evl.distributed.jms.batch;

import java.net.URISyntaxException;
import java.util.concurrent.atomic.AtomicInteger;
import org.eclipse.epsilon.evl.distributed.execute.data.DistributedEvlBatch;
import org.eclipse.epsilon.evl.distributed.jms.EvlModuleJmsMaster;
import org.eclipse.epsilon.evl.execute.atoms.ConstraintContextAtom;

/**
 * Batch-based approach, requiring only indices of the deterministic
 * jobs created from ConstraintContext and element pairs.
 * 
 * @see ConstraintContextAtom
 * @see DistributedEvlBatch
 * @author Sina Madani
 * @since 1.6
 */
public class EvlModuleJmsMasterBatch extends EvlModuleJmsMaster {
	
	protected final BatchJobSplitter splitter;
	
	public EvlModuleJmsMasterBatch(int expectedWorkers, double masterProportion, double batchFactor, boolean shuffle, String host, int sessionID) throws URISyntaxException {
		super(expectedWorkers, host, sessionID);
		double mp = masterProportion >= 0 && masterProportion <= 1 ? masterProportion : 1 / (1 + expectedSlaves);
		double bf = batchFactor >= 0 && batchFactor <= 1 ? batchFactor : 0.008;
		splitter = new BatchJobSplitter(mp, shuffle, bf);
	}
	
	@Override
	protected void processJobs(AtomicInteger workersReady) throws Exception {
		waitForWorkersToConnect(workersReady);
		
		sendAllJobs(splitter.getWorkerJobs());
		
		log("Began processing own jobs");
		executeJob(splitter.getMasterJobs());
		log("Finished processing own jobs");
	}
}
