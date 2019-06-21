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
import org.eclipse.epsilon.evl.distributed.execute.data.DistributedEvlBatch;
import org.eclipse.epsilon.evl.distributed.flink.EvlModuleFlinkMaster;
import org.eclipse.epsilon.evl.distributed.flink.format.FlinkInputFormat;

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
public class EvlModuleFlinkSubset extends EvlModuleFlinkMaster<DistributedEvlBatch> {

	public EvlModuleFlinkSubset() {
		super();
	}

	public EvlModuleFlinkSubset(int distributedParallelism, boolean shuffle, double batchFactor) {
		super(distributedParallelism, 0, shuffle, batchFactor);
	}
	
	public EvlModuleFlinkSubset(int distributedParallelism, boolean shuffle, int batchSize) {
		super(distributedParallelism, 0, shuffle, batchSize);
	}

	@SuppressWarnings("unchecked")
	@Override
	protected DataSource<DistributedEvlBatch> getProcessingPipeline(ExecutionEnvironment execEnv) throws Exception {
		return execEnv
			.createInput(
				new FlinkInputFormat<>((List<DistributedEvlBatch>) jobSplitter.getWorkerJobs()),
				TypeInformation.of(DistributedEvlBatch.class)
			);
	}
}
