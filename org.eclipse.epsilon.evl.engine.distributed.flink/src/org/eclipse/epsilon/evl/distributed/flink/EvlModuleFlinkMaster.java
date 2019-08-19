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
import java.util.Objects;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.eclipse.epsilon.eol.exceptions.EolRuntimeException;
import org.eclipse.epsilon.eol.execute.context.IEolContext;
import org.eclipse.epsilon.evl.distributed.EvlModuleDistributedMaster;
import org.eclipse.epsilon.evl.distributed.execute.context.EvlContextDistributedMaster;
import org.eclipse.epsilon.evl.distributed.execute.data.SerializableEvlResultAtom;
import org.eclipse.epsilon.evl.distributed.flink.execute.context.EvlContextFlinkMaster;
import org.eclipse.epsilon.evl.distributed.strategy.JobSplitter;

/**
 * Convenience base class for Flink EVL modules.
 * 
 * @param <D> The type of inputs used for {@link EvlFlinkFlatMapFunction}.
 * 
 * @author Sina Madani
 * @since 1.6
 */
public abstract class EvlModuleFlinkMaster<D extends Serializable> extends EvlModuleDistributedMaster {

	private ExecutionEnvironment executionEnv;
	
	protected EvlModuleFlinkMaster(EvlContextFlinkMaster context, JobSplitter<?, D> jobSplitter) {
		super(Objects.requireNonNull(context), jobSplitter);
	}

	@Override
	protected final void prepareExecution() throws EolRuntimeException {
		super.prepareExecution();		
		EvlContextDistributedMaster context = getContext();
		executionEnv = ExecutionEnvironment.getExecutionEnvironment();
		int parallelism = context.getDistributedParallelism();
		if (parallelism < 1 && parallelism != ExecutionConfig.PARALLELISM_DEFAULT) {
			context.setDistributedParallelism(parallelism = ExecutionConfig.PARALLELISM_DEFAULT);
		}
		executionEnv.setParallelism(parallelism);
	}
	
	protected abstract DataSource<D> getProcessingPipeline(final ExecutionEnvironment execEnv) throws Exception;
	
	@Override
	protected final void checkConstraints() throws EolRuntimeException {
		try {
			Configuration config = getContext().getJobConfiguration();
			String outputPath = getContext().getOutputPath();
			executionEnv.getConfig().setGlobalJobParameters(config);
			DataSet<SerializableEvlResultAtom> pipeline = getProcessingPipeline(executionEnv)
				.flatMap(new EvlFlinkFlatMapFunction<>());
			
			if (outputPath != null && !outputPath.isEmpty()) {
				pipeline.writeAsText(outputPath, WriteMode.OVERWRITE);
				executionEnv.execute();
			}
			else {
				boolean valid = deserializeResults(pipeline.collect());
				assert valid;
			}
		}
		catch (Exception ex) {
			EolRuntimeException.propagate(ex);
		}
	}

	@Override
	public void setContext(IEolContext context) {
		if (context instanceof EvlContextFlinkMaster) {
			super.setContext(context);
		}
		else if (context != null) {
			throw new IllegalArgumentException(
				"Invalid context type: expected "+EvlContextFlinkMaster.class.getName()
				+ " but got "+context.getClass().getName()
			);
		}
	}
	
	@Override
	public EvlContextFlinkMaster getContext() {
		return (EvlContextFlinkMaster) super.getContext();
	}
}
