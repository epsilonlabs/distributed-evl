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
import org.eclipse.epsilon.eol.exceptions.EolRuntimeException;
import org.eclipse.epsilon.evl.distributed.EvlModuleDistributedMaster;
import org.eclipse.epsilon.evl.distributed.crossflow.execute.context.EvlContextCrossflowMaster;
import org.eclipse.scava.crossflow.runtime.Mode;

public class EvlModuleCrossflowMaster extends EvlModuleDistributedMaster {
	
	public EvlModuleCrossflowMaster(EvlContextCrossflowMaster context) {
		super(context);
		crossflow = new DistributedEVL(Mode.MASTER_BARE);
		crossflow.getConfigConfigSource().module = this;
		crossflow.setInstanceId(getContext().getInstanceId());
	}
	
	DistributedEVL crossflow;
	Collection<? extends Serializable> workerJobs;
	Serializable config;
	
	@Override
	public void prepareWorkers(Serializable configuration) throws Exception {
		this.config = configuration;
	}
	
	@Override
	protected void executeWorkerJobs(Collection<? extends Serializable> jobs) throws EolRuntimeException {
		this.workerJobs = jobs;
		try {
			crossflow.run(5000L);
			crossflow.awaitTermination();
			crossflow.getResultSink();
		}
		catch (Exception ex) {
			throw new EolRuntimeException(ex);
		}
	}
	
	@Override
	public EvlContextCrossflowMaster getContext() {
		return (EvlContextCrossflowMaster) super.getContext();
	}
}
