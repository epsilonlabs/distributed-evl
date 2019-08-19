/*********************************************************************
 * Copyright (c) 2018 The University of York.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
**********************************************************************/
package org.eclipse.epsilon.evl.distributed.flink.batch;

import java.util.List;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.eclipse.epsilon.erl.execute.data.JobBatch;
import org.eclipse.epsilon.evl.distributed.flink.EvlModuleFlinkMaster;
import org.eclipse.epsilon.evl.distributed.flink.execute.context.EvlContextFlinkMaster;
import org.eclipse.epsilon.evl.distributed.flink.format.FlinkInputFormat;
import org.eclipse.epsilon.evl.distributed.strategy.BatchJobSplitter;

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
public class EvlModuleFlinkSubset extends EvlModuleFlinkMaster<JobBatch> {
	
	public EvlModuleFlinkSubset(EvlContextFlinkMaster context, BatchJobSplitter jobSplitter) {
		super(context, jobSplitter);
	}

	@SuppressWarnings("unchecked")
	@Override
	protected DataSource<JobBatch> getProcessingPipeline(ExecutionEnvironment execEnv) throws Exception {
		return execEnv
			.createInput(
				new FlinkInputFormat<>((List<JobBatch>) jobSplitter.getWorkerJobs()),
				TypeInformation.of(JobBatch.class)
			);
	}
}
