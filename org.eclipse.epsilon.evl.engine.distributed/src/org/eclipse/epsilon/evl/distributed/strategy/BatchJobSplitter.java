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

import java.util.Collection;
import java.util.List;
import org.eclipse.epsilon.eol.exceptions.EolRuntimeException;
import org.eclipse.epsilon.erl.execute.data.JobBatch;
import org.eclipse.epsilon.evl.distributed.execute.context.EvlContextDistributedMaster;

/**
 * This distribution strategy splits the model elements into a preset
 * number of batches based on the parallelism, so that each worker knows
 * exactly which elements to evaluate in advance. The only data sent
 * over the wire as input is the start and end index of the jobs for
 * each node. It is expected that the distribution runtime algorithm
 * sends each batch to a different node.
 * 
 * @author Sina Madani
 * @since 1.6
 */
public class BatchJobSplitter extends JobSplitter<JobBatch, JobBatch> {
	
	protected final double batchSize;
	
	public BatchJobSplitter(EvlContextDistributedMaster context, double masterProportion, boolean shuffle, double batchSize) {
		super(context, masterProportion, shuffle);
		this.batchSize = sanitizeBatchSize(batchSize);
	}
	
	/**
	 * Validates the batchSize parameter, providing a default fallback value if out of bounds.
	 * 
	 * @param The raw input batchSize.
	 * @return Either a positive integer or a value between 0 and 1.
	 */
	protected double sanitizeBatchSize(double bf) {
		if (Math.min(0, bf) < 0) return context.getParallelism();
		else if (bf == 0) return 1;
		else return bf;
	}
	
	@Override
	protected Collection<JobBatch> convertToWorkerJobs(Collection<JobBatch> jobs) throws EolRuntimeException {
		return masterJobs;
	}

	@Override
	protected List<JobBatch> getAllJobs() throws EolRuntimeException {
		List<?> jobList = context.getModule().getAllJobs();
		final int numTotalJobs = jobList.size(), chunks;
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