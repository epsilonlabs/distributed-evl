/*********************************************************************
 * Copyright (c) 2018 The University of York.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
**********************************************************************/
package org.eclipse.epsilon.evl.distributed.flink;

import java.io.Serializable;
import java.util.Collection;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.ExecutionConfig.GlobalJobParameters;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.eclipse.epsilon.eol.exceptions.EolRuntimeException;
import org.eclipse.epsilon.evl.distributed.EvlModuleDistributedMaster;
import org.eclipse.epsilon.evl.distributed.flink.execute.context.EvlContextFlinkMaster;

/**
 * 
 * @author Sina Madani
 * @since 1.6
 */
public class EvlModuleFlinkMaster extends EvlModuleDistributedMaster {

	private final ExecutionEnvironment executionEnv = ExecutionEnvironment.getExecutionEnvironment();

	public EvlModuleFlinkMaster(EvlContextFlinkMaster context) {
		super(context);
	}
	
	@Override
	public void prepareWorkers(Serializable configuration) throws Exception {
		EvlContextFlinkMaster context = getContext();
		int parallelism = context.getDistributedParallelism();
		if (parallelism < 1 && parallelism != ExecutionConfig.PARALLELISM_DEFAULT) {
			context.setDistributedParallelism(parallelism = ExecutionConfig.PARALLELISM_DEFAULT);
		}
		executionEnv.setParallelism(parallelism);
		executionEnv.getConfig().setGlobalJobParameters((GlobalJobParameters) configuration);
	}
	
	@Override
	protected final void executeWorkerJobs(Collection<? extends Serializable> jobs) throws EolRuntimeException {
		try {
			EvlContextFlinkMaster context = getContext();
			String outputPath = context.getOutputPath();
			
			DataSet<? extends Serializable> pipeline =
				executionEnv.fromCollection(jobs).flatMap(new EvlFlinkFlatMapFunction<>());
			
			if (outputPath != null && !outputPath.isEmpty()) {
				pipeline.writeAsText(outputPath, WriteMode.OVERWRITE);
				executionEnv.execute();
			}
			else {
				context.executeJob(pipeline.collect());
			}
		}
		catch (Exception ex) {
			EolRuntimeException.propagate(ex);
		}
	}
	
	@Override
	public EvlContextFlinkMaster getContext() {
		return (EvlContextFlinkMaster) super.getContext();
	}
}
