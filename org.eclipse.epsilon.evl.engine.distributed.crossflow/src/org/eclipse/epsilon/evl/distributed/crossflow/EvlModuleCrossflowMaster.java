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
import java.util.List;
import org.eclipse.epsilon.eol.exceptions.EolRuntimeException;
import org.eclipse.epsilon.eol.execute.context.IEolContext;
import org.eclipse.epsilon.evl.distributed.EvlModuleDistributedMaster;
import org.eclipse.epsilon.evl.distributed.crossflow.execute.context.EvlContextCrossflowMaster;
import org.eclipse.epsilon.evl.distributed.strategy.JobSplitter;
import org.eclipse.scava.crossflow.runtime.Mode;

public abstract class EvlModuleCrossflowMaster extends EvlModuleDistributedMaster {
	
	public EvlModuleCrossflowMaster(EvlContextCrossflowMaster context, JobSplitter<?, ?> strategy) {
		super(context, strategy);
	}

	public List<? extends Serializable> getWorkerJobs() throws EolRuntimeException {
		return jobSplitter.getWorkerJobs();
	}
	
	@Override
	protected void checkConstraints() throws EolRuntimeException {
		try {
			DistributedEVL crossflow = new DistributedEVL(Mode.MASTER_BARE);
			crossflow.setInstanceId(getContext().getInstanceId());
			crossflow.getConfigConfigSource().masterModule = this;
			crossflow.run(5000L);
			crossflow.awaitTermination();
			crossflow.getResultSink();
		}
		catch (Exception ex) {
			throw new EolRuntimeException(ex);
		}
	}
	
	@Override
	protected boolean deserializeResults(Object response) throws EolRuntimeException {
		return super.deserializeResults(response instanceof ValidationResult ?
			((ValidationResult) response).getAtoms() : response
		);
	}
	
	@Override
	public EvlContextCrossflowMaster getContext() {
		return (EvlContextCrossflowMaster) super.getContext();
	}
	
	@Override
	public void setContext(IEolContext context) {
		if (context instanceof EvlContextCrossflowMaster) {
			super.setContext(context);
		}
		else if (context != null) {
			throw new IllegalArgumentException(
				"Invalid context type: expected "+EvlContextCrossflowMaster.class.getName()
				+ " but got "+context.getClass().getName()
			);
		}
	}
}
