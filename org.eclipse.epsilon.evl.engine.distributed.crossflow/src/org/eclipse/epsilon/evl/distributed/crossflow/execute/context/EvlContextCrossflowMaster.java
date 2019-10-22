/*********************************************************************
 * Copyright (c) 2019 The University of York.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
**********************************************************************/
package org.eclipse.epsilon.evl.distributed.crossflow.execute.context;

import org.eclipse.epsilon.eol.exceptions.EolRuntimeException;
import org.eclipse.epsilon.evl.distributed.crossflow.*;
import org.eclipse.epsilon.evl.distributed.execute.context.EvlContextDistributedMaster;
import org.eclipse.epsilon.evl.distributed.strategy.JobSplitter;

public class EvlContextCrossflowMaster extends EvlContextDistributedMaster {
	
	protected String instanceID;

	public EvlContextCrossflowMaster(int localParallelism, int distributedParallelism, JobSplitter splitter, String instanceId) {
		super(localParallelism, distributedParallelism, splitter);
		this.instanceID = instanceId;
	}

	public String getInstanceId() {
		return this.instanceID;
	}
	
	@Override
	public Object executeJob(Object job) throws EolRuntimeException {
		if (job instanceof ValidationData) {
			job = ((ValidationData) job).getData();
		}
		else if (job instanceof ValidationResult) {
			job = ((ValidationResult) job).getAtoms();
		}
		return super.executeJob(job);
	}
	
	@Override
	public EvlModuleCrossflowMaster getModule() {
		return (EvlModuleCrossflowMaster) super.getModule();
	}
}
