/*********************************************************************
 * Copyright (c) 2019 The University of York.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
**********************************************************************/
package org.eclipse.epsilon.evl.distributed.crossflow.batch;

import java.util.List;
import org.eclipse.epsilon.eol.exceptions.EolRuntimeException;
import org.eclipse.epsilon.erl.execute.data.JobBatch;
import org.eclipse.epsilon.evl.distributed.crossflow.EvlModuleCrossflowMaster;
import org.eclipse.epsilon.evl.distributed.crossflow.execute.context.EvlContextCrossflowMaster;
import org.eclipse.epsilon.evl.distributed.strategy.JobSplitter;

/**
 * 
 *
 * @author Sina Madani
 * @since 1.6
 */
public class EvlModuleCrossflowMasterBatch extends EvlModuleCrossflowMaster {
	
	public EvlModuleCrossflowMasterBatch(EvlContextCrossflowMaster context, JobSplitter<?, ?> strategy) {
		super(context, strategy);
	}

	@SuppressWarnings("unchecked")
	@Override
	public List<? extends JobBatch> getWorkerJobs() throws EolRuntimeException {
		return (List<? extends JobBatch>) super.getWorkerJobs();
	}
}
