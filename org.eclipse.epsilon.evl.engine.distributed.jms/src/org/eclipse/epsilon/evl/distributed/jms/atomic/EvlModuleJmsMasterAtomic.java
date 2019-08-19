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

import java.util.concurrent.atomic.AtomicInteger;
import org.eclipse.epsilon.evl.distributed.execute.data.SerializableEvlInputAtom;
import org.eclipse.epsilon.evl.distributed.jms.EvlModuleJmsMaster;
import org.eclipse.epsilon.evl.distributed.jms.execute.context.EvlContextJmsMaster;
import org.eclipse.epsilon.evl.distributed.strategy.AtomicJobSplitter;
import org.eclipse.epsilon.evl.execute.atoms.ConstraintContextAtom;

/**
 * Atom-based approach, sending the Serializable ConstraintContext and model element
 * pairs to workers.
 * 
 * @see SerializableEvlInputAtom
 * @see ConstraintContextAtom
 * @author Sina Madani
 * @since 1.6
 */
public class EvlModuleJmsMasterAtomic extends EvlModuleJmsMaster {
	
	public EvlModuleJmsMasterAtomic(EvlContextJmsMaster context, AtomicJobSplitter strategy) {
		super(context, strategy);
	}

	@Override
	protected void processJobs(AtomicInteger workersReady) throws Exception {
		waitForWorkersToConnect(workersReady);
		
		sendAllJobs(jobSplitter.getWorkerJobs());
		
		log("Began processing own jobs");
		executeJob(jobSplitter.getMasterJobs());
		log("Finished processing own jobs");
	}
}
