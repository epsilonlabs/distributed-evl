/*********************************************************************
 * Copyright (c) 2019 The University of York.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
**********************************************************************/
package org.eclipse.epsilon.evl.distributed.strategy;

import java.util.List;
import org.eclipse.epsilon.eol.exceptions.EolRuntimeException;
import org.eclipse.epsilon.erl.execute.data.JobBatch;
import org.eclipse.epsilon.evl.distributed.execute.context.EvlContextDistributedMaster;

/**
 * 
 * @author Sina Madani
 * @since 1.6
 */
public class BatchJobSplitter extends JobSplitter<JobBatch, JobBatch> {
	
	protected final double batchSize;
	
	public BatchJobSplitter(EvlContextDistributedMaster context, double masterProportion, boolean shuffle, double batchSize) {
		super(context, masterProportion, shuffle);
		this.batchSize = sanitizeBatchSize();
	}
	
	/**
	 * Validates the batchSize parameter, providing a default fallback value if out of bounds.
	 * @return A positive value.
	 */
	protected double sanitizeBatchSize() {
		if (Math.min(0, masterProportion) < 0) return context.getParallelism();
		else if (masterProportion == 0) return 1;
		else return masterProportion;
	}
	
	@Override
	protected List<JobBatch> convertToWorkerJobs(List<JobBatch> masterJobs) throws EolRuntimeException {
		return masterJobs;
	}

	@Override
	protected List<JobBatch> getAllJobs() throws EolRuntimeException {
		final int numTotalJobs = getAllJobs().size(), chunks;
		if (this.batchSize >= 1) {
			chunks = (int) batchSize;
		}
		else {
			final int adjusted = Math.max(context.getDistributedParallelism(), 1);
			chunks = (int) (numTotalJobs * (batchSize / adjusted));
		}
		return JobBatch.getBatches(numTotalJobs, chunks);
	}
}